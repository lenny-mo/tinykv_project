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
// Match追随者已知的与领导者一致的最高日志条目的索引，
// Next 领导者下一次尝试发送给追随者的日志条目的索引
type Progress struct {
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

	// 避免transfer timeout 没有设置
	if raft.transferTimeout == 0 {
		raft.transferTimeout = raft.heartbeatTimeout * 3
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
	// Your Code Here (2A).DONE
	// etcd 在这个函数有三个步骤
	// 	- 1. 获取要发送的日志片段的最后一条日志的索引和term
	// 	- 2. 如果获取日志失败,则构造snapshot消息发送
	// 	- 3. 发送日志条目给其他节点后,更新对应节点的 Progress 信息
	//
	// reference: https://github.com/etcd-io/raft/blob/main/raft.go#L591
	// 1 根据Progress中的Next字段获取前一条日志的term，这个是为了给follower 校验
	// 在这个上下文中，Match 表示 follower 已经匹配的日志条目的最大索引，而不是待发送的最后一个索引。
	// 所以为了更清晰地区分两者的含义，通常会选择用 Next-1 来表示 follower 上次匹配的最后一个日志条目的索引。
	lastIndex, nextIndex := r.Prs[to].Next-1, r.Prs[to].Next
	lastTerm, errt := r.RaftLog.Term(lastIndex)

	if errt != nil { // 2 如果在获取NExt前一个日志的时候出错，发送snapshot
		snapshot, err := r.RaftLog.storage.Snapshot()
		if err != nil {
			return false // 发送失败
		}
		msg := pb.Message{
			MsgType:  pb.MessageType_MsgSnapshot, // 在'sendAppend'中，如果领导者无法获取Term或Entries， 领导者将通过发送'MessageType_MsgSnapshot'类型的消息请求快照。
			To:       to,
			From:     r.id,
			Term:     r.Term,
			Snapshot: &snapshot,
			Commit:   r.RaftLog.committed,
		}
		r.msgs = append(r.msgs, msg)
		r.Prs[to].Next = snapshot.Metadata.Index + 1 // 更新next，match不确定，不好更新
		return true
	}

	// 3 构造日志slice发送给目标结点，并且更新结点进度
	// etcd 中获取的时候是从Next字段开始往后取，直到达到单条消息承载的最大日志条数
	// 在这里我们直接从next开始往后一直遍历到entries末尾
	ents := []*pb.Entry{}

	// 获取next log在entries 中的下标
	for i := nextIndex - r.RaftLog.firstIndex; i < uint64(len(r.RaftLog.entries)); i++ {
		ents = append(ents, &r.RaftLog.entries[i])
	}
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppend, // 包含要复制的日志条目
		From:    r.id,
		To:      to,
		Commit:  r.RaftLog.committed,
		LogTerm: lastTerm, // 当前leader结点的日志中记录的lastindex 对应的任期, 后续发给follower的时候用于对比是否和follower的一只
		Index:   lastIndex,
		Entries: ents,
	}
	r.msgs = append(r.msgs, msg)
	r.Prs[to].Next = r.RaftLog.LastIndex() + 1 // 因为已经把从next开始的所有日志都发送，所以要下次要从last+1开始
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).DONE
	// 根据etcd 的描述，在发送heartbeat的时候需要求对方match和自身commit的最小值
	// 防止follower对于尚未匹配的日志进行强制提交，有可能会破坏一致性
	// 同时，心跳包要求数据量尽可能的小
	//
	// reference: https://github.com/etcd-io/raft/blob/main/raft.go#L669
	var sendCommit uint64
	if r.RaftLog.committed > r.Prs[to].Match {
		sendCommit = r.Prs[to].Match
	} else {
		sendCommit = r.RaftLog.committed
	}

	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		Commit:  sendCommit,
		From:    r.id,
		Term:    r.Term,
	}
	r.msgs = append(r.msgs, msg)
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
		r.triggerHeartbeat() // 发送心跳
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

func (r *Raft) triggerHeartbeat() {
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
	r.reset(term)
	r.Lead = lead // 在reset之后，因为rest会把当前leader设置为None
}

// reference: https://github.com/etcd-io/raft/blob/main/raft.go#L873
//
// 如果任期号与当前不同，说明开启新任期，重置投票；
//
// 领导设置为None
// 重置选举和心跳的时间进度；
// 重新设置选举超时时间: 这个字段在newraft函数内会初始化;
//
// 如果存在leader转移，终止，
// 重置所有结点的进度, 和newraft函数内的差不多,
//
//	正在进行的配置变更（由该节点发起）都应该被重置或放弃
func (r *Raft) reset(term uint64) {
	if r.Term != term { // 如果任期不同，说明已经开启了一个新任期
		r.Vote = None // 意味着一个新的任期已经开始，选票重新设置为None
		r.Term = term
	}
	r.Lead = None

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
	// etcd内部会有重新创建map的举动，
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
	// Your Code Here (2A).DONE
	// 参考etcd的becomecandidate func
	//
	// reference: https://github.com/etcd-io/raft/blob/main/raft.go#L873
	if r.State == StateLeader {
		panic(errors.New("invalid transition [leader -> candidate]")) // 不能从leader转移到candidate
	}

	r.reset(r.Term + 1)
	r.Vote = r.id
	r.votes[r.id] = true // 自己给自己投票
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).DONE
	// NOTE: Leader should propose a noop entry on its
	//
	// reference: https://github.com/etcd-io/raft/blob/main/raft.go#L902
	if r.State == StateFollower {
		panic(errors.New("invalid transition [follower -> leader]"))
	}

	// 一旦候选人赢得了选举，它就会转变为领导者。在这个转变过程中，它的任期号 r.Term 不会发生变化。它继续使用在选举开始时增加的任期号。
	// 重置结点的状态，并且把leader设置为自己
	r.reset(r.Term)
	r.State = StateLeader
	r.Lead = r.id

	// 添加一个空的日志条目。这是为了强制 Raft 提交到当前的任期号。
	// 这个行为和开头的should propose a noop entry 是一样的
	currentLastIndex := r.RaftLog.LastIndex()
	noopEntry := pb.Entry{
		Term:      r.Term,
		Index:     currentLastIndex + 1,
		Data:      []byte{},
		EntryType: pb.EntryType_EntryNormal, // 默认0就是normal
	}
	r.RaftLog.entries = append(r.RaftLog.entries, noopEntry)

	// 发送noopEntry到其他的结点
	// TODO: 下面的这个操作考虑做成原子操作, 比如有可能断电，导致消息发送后但是没有更新结点的进度
	// 类似两阶段提交
	for i := range r.Prs {
		if i == r.id {
			continue
		}
		r.sendAppend(i)
	}

	// 进度更新
	// 在etcd 代码中，pr.BecomeReplicate() 会默认直接从LastIndex+1 日志开始发送
	// 而不是检查follower是否已经和自己同步到lastIndex
	// 在我们的代码中，我也是这样操作
	for i := range r.Prs {
		if i == r.id {
			r.Prs[i].Match = currentLastIndex + 1
			r.Prs[i].Next = currentLastIndex + 2
		} else {
			// 默认其他结点和自己同步的Index是0
			r.Prs[i].Next = currentLastIndex + 2
		}
	}

	// 针对单结点的raft集群，提交它的最新Index, 如果不添加，
	// leader 无法提交（commit）任何新的日志条目
	if len(r.Prs) == 1 {
		r.RaftLog.committed = r.Prs[r.id].Match
	}

	// 针对集群更改日志的变动
	// 参考etcd，
	// 这行代码的意思是：将等待应用的配置更改的索引更新为当前日志中最新条目的索引。
	// 简单来说，它在告诉 Raft 节点：“我们有一个新的配置更改需要处理，它就在我们日志的最末端。”
	// 这是一种安全措施，在新的配置更改被复制到足够多的节点之前，不会有新的配置更改被提出
	r.PendingConfIndex = currentLastIndex
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	// 参考etcd的step方法，可笑的是，etcd并不是根据当前结点的状态进行移动，而是根据消息的term和当前结点的temr
	//
	// reference: https://github.com/etcd-io/raft/blob/main/raft.go#L1051
	// 在 Raft 中，当节点收到 Term 更高的消息时，需要将自身状态转变为 Follower。
	// 因为该节点接收到了一个 Term 更高的 Leader 的消息，所以必须遵循新 Leader 的领导。
	// 因此，对于 r.becomeFollower() 函数的调用，第二个参数 None 是用来表示当前 Leader 为 None，
	// 因为节点将会根据收到的消息中的信息来指定新的 Leader，而不是将该消息中的来源节点 m.From 作为 Leader。
	if m.Term > r.Term {
		r.becomeFollower(m.Term, None)
	}

	// TODO
	switch r.State {
	case StateFollower:
		r.stepFollower(m)
	case StateCandidate:
		r.stepCandidate(m)
	case StateLeader:
		r.stepLeader(m)
	}
	return nil
}

func (r *Raft) stepFollower(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.startElection() // todo
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgRequestVote:
		r.handleVote(m) // todo
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m) // 2C
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	}
	return nil
}

func (r *Raft) stepCandidate(m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgAppend:
		if m.Term >= r.Term {
			r.becomeFollower(m.Term, m.From)
			r.handleAppendEntries(m)
		}
	case pb.MessageType_MsgHup:
		r.startElection()
	case pb.MessageType_MsgRequestVoteResponse:
		r.handleVote(m)
	case pb.MessageType_MsgRequestVote:
		r.handleVote(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	}
}

func (r *Raft) stepLeader(m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgAppend:
		if m.Term > r.Term {
			r.becomeFollower(m.Term, m.From)
			r.handleAppendEntries(m)
		}
	case pb.MessageType_MsgRequestVote:
		r.handleVote(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgBeat:
		for id := range r.Prs {
			r.sendHeartbeat(id)
		}
		r.heartbeatElapsed = 0
	}
}

// TODO
func (r *Raft) startElection() error {

	return nil
}

// TODO
func (r *Raft) handleVote(m pb.Message) error {
	return nil
}

// handleAppendEntries handle AppendEntries RPC request
//
// 处理之前的sendAppend函数
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).DONE
	// 根据etcd，这个函数有三个步骤
	// 1. 如果收到的日志索引小于已提交的最新日志索引，说明这个日志已经被提交了，直接向消息来源发送已提交的最新日志索引。
	// 2. 尝试将收到的日志条目追加到本节点的日志中，如果追加成功，则向消息来源发送已追加的最新日志索引。
	// 2.1 在 maybeAppend() 方法中，follower 会进一步检查消息的 term 是否匹配当前的 term
	// 3. 如果日志追加失败，记录拒绝消息，并返回一个提示信息给消息来源，以便 Leader 能够得知日志匹配的最大 (index, term) 的猜测值，以便快速匹配日志。
	// 如果追加成功，返回消息
	//
	// reference: handleAppendEntries https://github.com/etcd-io/raft/blob/main/raft.go#L1733
	// reference: maybeAppend https://github.com/etcd-io/raft/blob/main/log.go#L109

	// 1. 判断leader 发送的append entries 的 index(leader记录的和当前结点的日志匹配索引) 是否小于当前的committd
	// 如果小于，返回消息并且附加当前的committed信息，并且告知leader 你的消息已经过时
	if m.Index < r.RaftLog.committed {
		msg := pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			Index:   r.RaftLog.committed,
			To:      m.From,
			From:    r.id,
			Reject:  true,
		}
		r.msgs = append(r.msgs, msg)
		return
	}

	r.electionElapsed = 0 // 重新倒计时
	r.Lead = m.From       // 设置为当前leader

	// 2. 尝试把消息的日志append 到当前结点的entries, 参考maybeAppend
	// 如果 index 超过当前结点的lastIndex, 说明发送的日志不连续，告诉leader, 发送 lastIndex+1给我
	if m.Index > r.RaftLog.LastIndex() {
		msg := pb.Message{
			To:      m.From,
			From:    r.id,
			Reject:  true,
			MsgType: pb.MessageType_MsgAppendResponse,
			Index:   r.RaftLog.LastIndex() + 1,
			Term:    r.Term,
		}
		r.msgs = append(r.msgs, msg)
		return
	}

	// 判断消息的Index(当前结点和leader match的日志索引) 是否和leader 的term相同
	// 如果在这个过程中发生意外，需要返回一条拒绝给Leader，并且返回hint index和 hint term给leader，具体的
	// 算法参考了etcd的handleAppendEntries
	if t, err := r.RaftLog.Term(m.Index); err != nil || t != m.Term {
		// 处理日志错误
		// 出现问题的时候，需要返回一个提示信息，参考etcd的 findConflictByTerm 函数
		// reference:https://github.com/etcd-io/raft/blob/main/log.go#L178
		// 这行代码首先计算 follower 自己日志的最后一个索引和 AppendEntries 中带的索引中的最小值作为一个提示索引。
		// 这个最小索引保证是 follower 自己日志中一定存在的一个索引。
		// 接着在这个提示索引处查找与 AppendEntries 中的 term 不匹配的第一个条目,并返回这个条目的索引和 term。
		// 这样就可以高效地找到 follower 自己日志与 leader 日志开始不一致的位置。
		hintIndex := min(m.Index, r.RaftLog.LastIndex())
		hintIndex, hintTerm := r.RaftLog.findConflictByTerm(hintIndex, m.LogTerm)
		msg := pb.Message{
			To:      m.From,
			From:    r.id,
			MsgType: pb.MessageType_MsgAppendResponse,
			Reject:  true,
			Index:   hintIndex, // 表示 follower 自己日志中最后一条日志的索引
			LogTerm: hintTerm,  // 表示该索引位置的日志条目的任期号
			Term:    r.Term,
		}
		r.msgs = append(r.msgs, msg)
		return
	}

	// 3. 经过上述的错误检测，进行日志追加
	// 找到新添加日志条目和现有日志之间的冲突索引
	// 将冲突索引后的日志追加到日志中
	//
	// reference: https://github.com/etcd-io/raft/blob/main/log.go#L109
	// 找到当前日志和leader日志的第一条冲突日志
	conflictIndex := r.RaftLog.findConflict(m.Entries)
	// 把从conflict位置开始的后续所有日志都添加到当前的日志中,此时就要调用truncateAndAppend函数
	// 如果conflict位置就在当前日志的最后 说明日志是连续的 直接把日志append就行
	// 如果conflict位置在当前日志的中间 则截断当前日志 使用拓展表达式

	// 如果conflictIndex == 0 把leader的日志全部添加到当前结点的日志
	if conflictIndex == 0 {
		for _, v := range m.Entries {
			r.RaftLog.entries = append(r.RaftLog.entries, *v)
		}
	} else {
		// 把从冲突位置开始的日志添加进入当前结点的entries
		beginIndex := m.Index + 1 // leader发过来的日志中的第一条的index
		// 如果conflictIndex不为零，则把从冲突位置开始的日志都添加到当前结点的日志中
		for _, v := range m.Entries[conflictIndex-beginIndex:] {
			r.RaftLog.entries = append(r.RaftLog.entries, *v)
		}
	}

	// 更新当前结点的committed
	latestIndex := m.Index + uint64(len(m.Entries)) // 添加完所有entries之后，当前结点的lastIndex
	minCommitted := min(m.Commit, latestIndex)
	if minCommitted > r.RaftLog.committed {
		r.RaftLog.committed = minCommitted
	}

	// 返回添加成功消息
	msg := pb.Message{
		To:      m.From,
		From:    r.id,
		MsgType: pb.MessageType_MsgAppendResponse,
		Index:   latestIndex,
		Term:    r.Term,
	}
	r.msgs = append(r.msgs, msg)
}

// copy from https://github.com/etcd-io/raft/blob/main/log.go#L178
func (l *RaftLog) findConflictByTerm(index uint64, term uint64) (uint64, uint64) {
	for ; index > 0; index-- {
		if ourTerm, err := l.Term(index); err != nil {
			// 如果在某个索引位置获取任期号失败,直接返回这个索引和0任期
			return index, 0
		} else if ourTerm <= term {
			// 如果在某个索引位置的任期号 <= 输入的任期号term,则返回这个索引和任期
			// 这意味着这个索引及之前的全部匹配
			return index, ourTerm
		}
	}
	return 0, 0
}

// 这个函数会在 Follower 端查找本地日志和 Leader 提供的日志之间第一个冲突的位置,
// 如果没有冲突则返回 0
func (l *RaftLog) findConflict(ents []*pb.Entry) uint64 {
	for _, ent := range ents {
		if !l.matchTerm(ent.Index, ent.Term) {
			return ent.Index
		}
	}
	return 0
}

func (l *RaftLog) matchTerm(i, term uint64) bool {
	t, err := l.Term(i)
	if err != nil {
		return false
	}
	return t == term
}

// handleHeartbeat handle Heartbeat RPC request
//
// follower 响应 leader 的心跳包
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).DONE
	// etcd的响应分为两步：
	// 1 根据心跳包，判断是否应该根据心跳包来更新当前follower的commit
	// 如果心跳包中的commit远超过当前的unstable entries，报错
	// 这也是为什么在发送心跳包的时候要求min(prs.Match, commit)
	// 2 发送一条响应信息给leader
	//
	// reference: https://github.com/etcd-io/raft/blob/main/raft.go#L1773

	// 1. 判断任期是否有效，如果无效拒绝
	if r.Term > m.Term { // return reject
		msg := pb.Message{
			MsgType: pb.MessageType_MsgHeartbeatResponse,
			Reject:  true,
			To:      m.From,
			From:    r.id,
			Term:    r.Term,
		}
		r.msgs = append(r.msgs, msg)
		return
	}

	// 2. 判断commit 是否超过lastIndex, 如果超过则报错
	if m.Commit > r.RaftLog.LastIndex() {
		// 报错
		msg := pb.Message{
			MsgType: pb.MessageType_MsgHeartbeatResponse,
			Reject:  true,
			To:      m.From,
			From:    r.id,
			Term:    r.Term,
		}
		r.msgs = append(r.msgs, msg)
		return
	}
	r.RaftLog.committed = m.Commit

	// 3. 重置election elapsed
	r.Lead = m.From
	r.electionElapsed = 0

	// 4. 发送响应信息
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		Reject:  false,
		To:      m.From,
		From:    r.id,
		Term:    r.Term,
	}
	r.msgs = append(r.msgs, msg)
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
