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
	"os"
	"sync"
	"time"

	"github.com/pingcap-incubator/tinykv/log"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

type lockedRand struct {
	mu   sync.Mutex
	rand *rand.Rand
}

func (r *lockedRand) Intn(n int) int {
	r.mu.Lock()
	v := r.rand.Intn(n)
	r.mu.Unlock()
	return v
}

var globalRand = &lockedRand{
	rand: rand.New(rand.NewSource(time.Now().UnixNano())),
}

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

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

func setLogOutput() {
	os.Remove(`raft.log`)
	logFile, err := os.OpenFile(`raft.log`, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		panic(err)
	}
	// 设置存储位置
	log.SetOutput(logFile)
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
	electionTimeout           int
	randomizedElectionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
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
	PendingConfIndex uint64
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	raftlog := newLog(c.Storage)
	r := &Raft{
		id:               c.ID,
		Lead:             None,
		RaftLog:          raftlog,
		Prs:              make(map[uint64]*Progress),
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
	}

	for _, id := range c.peers {
		r.Prs[id] = &Progress{}
	}
	r.becomeFollower(r.Term, None)
	setLogOutput()
	return r
}

func (r *Raft) quorum() int {
	return len(r.Prs)/2 + 1
}

func (r *Raft) send(m pb.Message) {
	m.From = r.id
	m.Term = r.Term
	r.msgs = append(r.msgs, m)
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
	commit := min(r.Prs[to].Match, r.RaftLog.committed)
	m := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		Commit:  commit,
	}
	r.send(m)
}

// tick advances the internal logical clock by a single tick.

func (r *Raft) resetRandomizedElectionTimeout() {
	r.randomizedElectionTimeout = r.electionTimeout + globalRand.Intn(r.electionTimeout)
}

func (r *Raft) promotable() bool {
	_, ok := r.Prs[r.id]
	return ok
}

func (r *Raft) pastElectionTimeout() bool {
	return r.electionElapsed >= r.randomizedElectionTimeout
}

func (r *Raft) tickHeartBeat() {
	r.electionElapsed++
	r.heartbeatElapsed++

	if r.electionElapsed > r.electionTimeout {
		//暂时怎么都不做
		r.electionElapsed = 0
	}

	if r.heartbeatElapsed >= r.heartbeatTimeout {
		r.heartbeatElapsed = 0
		r.Step(pb.Message{From: r.id, MsgType: pb.MessageType_MsgBeat})
	}
}

func (r *Raft) tickElection() {
	r.electionElapsed++

	if r.promotable() && r.pastElectionTimeout() {
		r.electionElapsed = 0
		r.Step(pb.Message{From: r.id, MsgType: pb.MessageType_MsgHup})
	}
}

func (r *Raft) tick() {
	// Your Code Here (2A).
	if r.State == StateLeader {
		r.tickHeartBeat()
	} else {
		r.tickElection()
	}
}

// 一些状态变化时需要重置的通用部分
func (r *Raft) reset(term uint64) {
	if term < r.Term {
		log.Fatal("reset with a less term")
	}

	if term > r.Term {
		r.Vote = None
		r.Term = term
	}

	r.Lead = None
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.resetRandomizedElectionTimeout()
	r.votes = make(map[uint64]bool)
}

func (r *Raft) campaign() {
	r.becomeCandidate()
	if r.quorum() == r.poll(r.id, true) { //投自己一票
		r.becomeLeader()
		return
	}

	for id := range r.Prs {
		if id == r.id {
			continue
		}
		log.Infof("%x send requestVote msg to %x", r.id, id)
		lastLogIndex := r.RaftLog.LastIndex()
		lastLogTerm := r.RaftLog.LastTerm()
		r.send(pb.Message{MsgType: pb.MessageType_MsgRequestVote,
			To: id, Term: r.Term,
			LogTerm: lastLogTerm, Index: lastLogIndex})
	}
}

func (r *Raft) handleRequestVote(m pb.Message) {
	if (r.Vote == None || m.Term > r.Term || r.Vote == m.From) && r.RaftLog.isUpToDate(m.Index, m.LogTerm) {
		log.Infof("%x [last logterm: %d, index: %d, vote: %x] grant vote for %x [logterm: %d, index: %d] at term %d",
			r.id, r.RaftLog.LastTerm(), r.RaftLog.LastIndex(), r.Vote, m.From, m.LogTerm, m.Index, r.Term)
		r.electionElapsed = 0
		r.Vote = m.From
		r.send(pb.Message{MsgType: pb.MessageType_MsgRequestVoteResponse, To: m.From})
	} else {
		log.Infof("%x [last logterm: %d, index: %d, vote: %x] reject vote for %x [logterm: %d, index: %d] at term %d",
			r.id, r.RaftLog.LastTerm(), r.RaftLog.LastIndex(), r.Vote, m.From, m.LogTerm, m.Index, r.Term)
		r.send(pb.Message{MsgType: pb.MessageType_MsgRequestVoteResponse, To: m.From, Reject: true})
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.reset(term)
	r.Lead = lead
	r.State = StateFollower
	log.Infof("%x became follower at term %v", r.id, r.Term)
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.reset(r.Term + 1)
	r.Vote = r.id
	r.State = StateCandidate
	log.Infof("%x became Candidate at term %v", r.id, r.Term)
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term

	r.reset(r.Term)
	r.Lead = r.id
	r.State = StateLeader

	//todo 发送一条空日志，这里先发一个心跳
	r.Step(pb.Message{MsgType: pb.MessageType_MsgBeat, From: r.id})
	log.Infof("%x became Leader at term %v", r.id, r.Term)
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	switch {
	case m.Term == 0:
		//本地消息，什么都不做
	case m.Term > r.Term:
		lead := m.From

		if m.MsgType == pb.MessageType_MsgRequestVote {
			//认为leader还活着，忽略这个消息，但是测试貌似不允许这样实现
			// inLease := r.Lead != None && r.electionElapsed < r.electionTimeout
			// if inLease {
			// 	log.Infof("%x ignore a MsgRequestVote from %x because lease not expired", r.id, m.From)
			// 	return nil
			// }
			lead = None //准备投票，暂还没有leader
		}

		log.Infof("%x [term: %d] received a %s message with higher term from %x [term: %d]",
			r.id, r.Term, m.MsgType, m.From, m.Term)
		r.becomeFollower(m.Term, lead)
	case m.Term < r.Term:
		//当App或者heartbeat消息的term低于当前节点的term时，响应一个消息，让发送者调整自己的状态
		if m.MsgType == pb.MessageType_MsgAppend {
			r.send(pb.Message{MsgType: pb.MessageType_MsgHeartbeatResponse, To: m.From})
		} else if m.MsgType == pb.MessageType_MsgHeartbeat {
			r.send(pb.Message{MsgType: pb.MessageType_MsgAppendResponse, To: m.From})
		} else {
			//其他消息直接忽略，并返回
			log.Infof("%x [term: %d] ignored a %s message with lower term from %x [term: %d]",
				r.id, r.Term, m.MsgType, m.From, m.Term)
			return nil
		}
	}

	//先一起处理所有类型成员都要响应的msg,即发起选举和投票
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		if r.State != StateLeader {
			r.campaign()
		} else {
			log.Infof("%x ignore a MsgHup because already a leader", r.id)
		}
		return nil
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
		return nil
	}

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

func (r *Raft) stepFollower(m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgHeartbeat:
		r.electionElapsed = 0
		r.Lead = m.From
		r.handleHeartbeat(m)
	}
}

func (r *Raft) stepCandidate(m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgRequestVoteResponse:
		r.handleRequestVoteResp(m)
	case pb.MessageType_MsgHeartbeat:
		r.becomeFollower(r.Term, m.From)
		r.handleHeartbeat(m)
	case pb.MessageType_MsgAppend:
		r.becomeFollower(r.Term, m.From)
		r.handleAppendEntries(m)
	}
}

func (r *Raft) stepLeader(m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgBeat:
		r.bcastHeartBeat()
	case pb.MessageType_MsgHeartbeat:
		log.Panicf("%x leader received a heartbeat from another leader %x at Term %d", r.id, m.From, r.Term)
	case pb.MessageType_MsgHeartbeatResponse:
		//暂时什么都不做
	case pb.MessageType_MsgPropose:
		r.bcastHeartBeat()
	}
}

func (r *Raft) bcastHeartBeat() {
	for id, _ := range r.Prs {
		if r.id == id {
			continue
		}
		r.sendHeartbeat(id)
	}
}

func (r *Raft) poll(id uint64, v bool) (granted int) {
	if v {
		log.Infof("%x received vote from %x at term %d", r.id, id, r.Term)
	} else {
		log.Infof("%x received rejection from %x at term %d", r.id, id, r.Term)
	}

	if _, ok := r.votes[id]; !ok {
		r.votes[id] = v
	}

	for _, voted := range r.votes {
		if voted {
			granted++
		}
	}
	return
}

func (r *Raft) handleRequestVoteResp(m pb.Message) {
	granted := r.poll(m.From, !m.Reject)
	if granted == r.quorum() {
		r.becomeLeader()
	} else if len(r.votes)-granted == r.quorum() {
		r.becomeFollower(r.Term, None)
	}
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	r.RaftLog.commitTo(m.Commit)
	r.send(pb.Message{MsgType: pb.MessageType_MsgHeartbeatResponse, To: m.From})
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
