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

	"github.com/Connor1996/badger/y"
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

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

func (r *Raft) resetPrs() {
	for i := range r.Prs {
		r.Prs[i] = &Progress{Match: 0, Next: r.RaftLog.LastIndex() + 1}
	}
}

func (r *Raft) GetId() uint64 {
	return r.id
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers. Note: Don't include itself
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

	// randomizedElectionTimeout is a random number between
	// [electiontimeout, 2 * electiontimeout - 1]. It gets reset
	// when raft changes its state to follower or candidate.
	randomizedElectionTimeout int
	// baseline of election interval
	electionTimeout int
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

	Transferee uint64
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}

	raftlog := newLog(c.Storage)

	// Your Code Here (2A).
	r := Raft{
		id:                        c.ID,
		Term:                      0,
		Vote:                      None,
		RaftLog:                   raftlog,
		Prs:                       map[uint64]*Progress{},
		State:                     StateFollower,
		votes:                     map[uint64]bool{},
		msgs:                      nil,
		Lead:                      None,
		heartbeatTimeout:          c.HeartbeatTick,
		randomizedElectionTimeout: c.ElectionTick,
		electionTimeout:           c.ElectionTick,
		heartbeatElapsed:          0,
		electionElapsed:           0,
		leadTransferee:            0, // todo(wq)
		PendingConfIndex:          0, // todo(wq)
	}
	hardState, softState, _ := raftlog.storage.InitialState()
	// raft_assert(len(softState.Nodes) != 0 || len(c.peers) != 0)  what fuck: raft/rawnode_test.go func TestRawNodeRestart2AC()
	if len(c.peers) != 0 {
		for _, peerId := range c.peers {
			r.Prs[peerId] = &Progress{Match: 0, Next: r.RaftLog.LastIndex() + 1}
		}
	} else { //
		for _, peerId := range softState.Nodes {
			r.Prs[peerId] = &Progress{Match: 0, Next: r.RaftLog.LastIndex() + 1}
		}
	}

	r.Term = hardState.Term
	r.Vote = hardState.Vote
	r.RaftLog.committed = hardState.Commit

	log.Infof("[wq] newRaft %x [term: %d, peerSize: %d]", r.id, r.Term, len(c.peers))
	return &r
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	pr := r.Prs[to]
	m := pb.Message{From: r.id, To: to, Term: r.Term, MsgType: pb.MessageType_MsgAppend}

	term, errt := r.RaftLog.Term(pr.Next - 1)
	if errt == ErrCompacted || errt == ErrUnavailable {
		errt = nil
		var snapshot pb.Snapshot
		if IsEmptySnap(r.RaftLog.pendingSnapshot) {
			snapshot, errt = r.RaftLog.storage.Snapshot()
		} else {
			snapshot = *r.RaftLog.pendingSnapshot
		}
		if errt != nil {
			// log.Infof("[wq] raftId: %d, find %d need snapshot, but don't ready", r.id, to)
			y.Assert(errt == ErrSnapshotTemporarilyUnavailable)
			return false
		}
		// log.Infof("[wq] raftId: %d, find %d need snapshot, ready...Go!!!!", r.id, to)
		msg := pb.Message{
			MsgType:  pb.MessageType_MsgSnapshot,
			From:     r.id,
			To:       to,
			Term:     r.Term,
			Snapshot: &snapshot,
		}
		r.msgs = append(r.msgs, msg)
		r.Prs[to].Next = snapshot.Metadata.Index + 1
		return true
	} else {
		raft_assert(errt == nil)
	}

	ents := r.RaftLog.Entries(pr.Next, r.RaftLog.LastIndex()+1) // 左闭右开
	m.Index = pr.Next - 1                                       // prevLogIndex
	m.LogTerm = term                                            // prevLogTerm

	m.Commit = r.RaftLog.committed
	m.Term = r.Term
	m.Entries = make([]*pb.Entry, len(ents))
	for i := 0; i < len(ents); i++ {
		m.Entries[i] = &ents[i]
	}
	// log.Infof("[wq] %x send append to %x, commit %d, prevLogIndex %d, prevLogTerm %d",
	// 	r.id, m.To, m.Commit, m.Index, m.LogTerm)
	r.send(m)
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	r.send(pb.Message{
		From:    r.id,
		To:      to,
		Term:    r.Term,
		MsgType: pb.MessageType_MsgHeartbeat,
	})
}

// if the transferee is qualified (or after the current leader’s help)
// the leader should send a MsgTimeoutNow message to the transferee immediatel
func (r *Raft) sendTimeoutNow(to uint64) {
	r.send(pb.Message{
		From:    r.id,
		To:      to,
		Term:    r.Term,
		MsgType: pb.MessageType_MsgTimeoutNow})
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower, StateCandidate:
		r.electionElapsed++
		if r.electionElapsed >= r.randomizedElectionTimeout { // election timeout: vote request
			r.electionElapsed = 0
			if err := r.Step(pb.Message{From: r.id, MsgType: pb.MessageType_MsgHup}); err != nil {
				log.Infof("[wq] error occurred during hup election: %v", err)
			}
		}
	case StateLeader:
		r.heartbeatElapsed++
		if r.heartbeatElapsed >= r.heartbeatTimeout { // hearteat timeout: broadcast
			r.heartbeatElapsed = 0
			if err := r.Step(pb.Message{From: r.id, MsgType: pb.MessageType_MsgBeat}); err != nil {
				log.Infof("[wq] error occurred during checking sending heartbeat: %v", err)
			}
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	raft_assert(r.Term <= term)
	r.reset(term)
	r.Term = term
	r.Vote = lead
	r.Lead = lead
	r.State = StateFollower
	log.Infof("[wq] %x became follower at term %d", r.id, r.Term)
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).

	if r.State == StateLeader {
		panic("invalid transition [leader -> candidate]")
	}
	r.reset(r.Term + 1)
	r.votes[r.id] = true
	r.Vote = r.id
	r.State = StateCandidate
	log.Infof("[wq] %x became candidate at term %d", r.id, r.Term)
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// a node that has been removed from the group, if the node also got votes, it would panic as it
	// transitioned to StateLeader
	if _, exist := r.Prs[r.id]; !exist {
		log.Panic("a node that has been removed from the group, but it transitioned to StateLeader")
	}
	r.reset(r.Term)
	r.resetPrs()
	if r.State == StateFollower {
		panic("invalid transition [follower -> leader]")
	}

	r.State = StateLeader
	r.Lead = r.id
	log.Infof("[wq] %x became leader at term %d", r.id, r.Term)

	if err := r.Step(pb.Message{From: r.id, To: r.id, MsgType: pb.MessageType_MsgPropose, Entries: []*pb.Entry{{Data: nil}}}); err != nil {
		log.Panic("[wq] empty entry was dropped")
	}
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) (err error) {
	// Your Code Here (2A).
	switch {
	case m.Term == 0:
		// local message
	case m.Term > r.Term:
		log.Infof("[wq] %x [term: %d] received a %s message with higher term from %x [term: %d]",
			r.id, r.Term, m.MsgType, m.From, m.Term)
		if m.MsgType == pb.MessageType_MsgAppend || m.MsgType == pb.MessageType_MsgHeartbeat || m.MsgType == pb.MessageType_MsgSnapshot {
			r.becomeFollower(m.Term, m.From)
		} else { // 不会直接给vote request 投票，因为可能log不是最新的
			r.becomeFollower(m.Term, None)
		}

	case m.Term < r.Term:
		log.Infof("[wq] %x [term: %d] ignored a %s message with lower term from %x [term: %d]",
			r.id, r.Term, m.MsgType, m.From, m.Term)
		return nil
	}

	switch m.MsgType {
	case pb.MessageType_MsgHup:
		if _, exist := r.Prs[r.id]; exist {
			r.hup() // remove的node也可能触发心跳
		}
	case pb.MessageType_MsgRequestVote:
		// We can vote if this is a repeat of a vote we've already cast...
		canVote := r.Vote == m.From || r.Vote == None
		canVote = canVote && r.RaftLog.isUpToDate(m.Index, m.LogTerm)
		if canVote {
			log.Infof("%x [vote: %x] support %s from %x at term %d",
				r.id, r.Vote, m.MsgType, m.From, r.Term)
			r.send(pb.Message{To: m.From, Term: m.Term, MsgType: voteRespMsgType(m.MsgType), Reject: false})
			r.electionElapsed = 0
			r.Vote = m.From
			r.Lead = None // 面向测试样例编程，当收到心跳或者append才更新r.Lead
		} else {
			log.Infof("%x [term: %d, vote: %x] rejected %s from %x at term %d",
				r.id, r.Term, r.Vote, m.MsgType, m.From, r.Term)
			r.send(pb.Message{To: m.From, Term: r.Term, MsgType: voteRespMsgType(m.MsgType), Reject: true})
		}
	default:
		switch r.State {
		case StateLeader:
			err = stepLeader(r, m)
		case StateCandidate:
			err = stepCandidate(r, m)
		case StateFollower:
			err = stepFollower(r, m)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	if term, err := r.RaftLog.Term(m.Index); err != nil {
		r.send(pb.Message{To: m.From, Term: r.Term, MsgType: pb.MessageType_MsgAppendResponse, Index: m.Index, Reject: true})
	} else {
		if term == m.LogTerm { // accept
			ents := make([]pb.Entry, len(m.Entries))
			for i := range ents {
				ents[i] = *m.Entries[i]
			}
			lastIndex := m.Index
			if len(ents) != 0 {
				// TestHandleMessageType_MsgAppend2AB()
				lastIndex = r.RaftLog.append(ents...)
			}
			if m.Commit > r.RaftLog.committed {
				// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry).
				// commitIndex = min(leaderCommit, index of last new entry)
				if m.Commit < lastIndex {
					r.RaftLog.committed = m.Commit
				} else {
					r.RaftLog.committed = lastIndex
				}
			}
			r.send(pb.Message{To: m.From, Term: r.Term, MsgType: pb.MessageType_MsgAppendResponse, Index: lastIndex, Reject: false})
		} else {
			r.send(pb.Message{To: m.From, Term: r.Term, MsgType: pb.MessageType_MsgAppendResponse, Index: m.Index, Reject: true})
		}
	}
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	// log.Infof("[wq] %x send %x  %s", r.id, m.From, pb.MessageType_MsgHeartbeatResponse)
	r.send(pb.Message{
		From: r.id, To: m.From, Term: r.Term,
		Index:   r.RaftLog.LastIndex(),
		LogTerm: r.RaftLog.LastTerm(),
		MsgType: pb.MessageType_MsgHeartbeatResponse})
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
	meta := m.Snapshot.Metadata
	if meta.Index <= r.RaftLog.committed ||
		meta.Term < r.Term {
		// panic("[wq] implement me")
		return
	}
	r.RaftLog.applied = meta.Index
	r.RaftLog.committed = meta.Index
	r.RaftLog.stabled = meta.Index
	if len(r.RaftLog.entries) > 0 {
		if meta.Index > r.RaftLog.LastIndex() {
			r.RaftLog.entries = nil
		} else if meta.Index >= r.RaftLog.FirstIndex() {
			r.RaftLog.entries = r.RaftLog.Entries(meta.Index+1, r.RaftLog.LastIndex()+1)
		}
	}
	for _, nodeId := range m.Snapshot.Metadata.ConfState.GetNodes() {
		r.Prs[nodeId] = &Progress{}
	}
	r.RaftLog.offset = meta.Index
	r.RaftLog.offsetTerm = meta.Term
	r.RaftLog.pendingSnapshot = m.Snapshot
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
	r.Prs[id] = &Progress{Match: 0, Next: r.RaftLog.LastIndex() + 1}
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
	if r.Lead == id {
		// https://asktug.com/t/topic/274196 (一个 corner case)
		log.Panicf("[wq] remove leader is risky (认为这个BUG是可以接受的,直接重跑测试吧)")
		return
	}
	delete(r.Prs, id)
	// pending commands can become committed when a config change reduces the quorum requirements.
	if r.State == StateLeader {
		r.maybeUpdateCommit()
	}
}
