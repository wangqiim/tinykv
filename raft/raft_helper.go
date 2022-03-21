// author: wangqi
// 该文件的目的
// 一是提供一些封装的方法和工具
// 二是将添加的方法和原方法通过文件隔离开，防止修改时影响到原因的测试

package raft

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"math/rand"

	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

//------------------error & assert-------------------------------------
var ErrMsgTermTooLow = errors.New("message term to low")
var ErrDefensiveAssert = errors.New("raft: defensive programming error")
var ErrNotEmplement = errors.New("something not emplement")

func raft_assert(stmt bool) {
	if !stmt {
		panic(ErrDefensiveAssert.Error())
	}
}

type VoteResult uint8

const (
	// VotePending indicates that the decision of the vote depends on future
	// votes, i.e. neither "yes" or "no" has reached quorum yet.
	VotePending VoteResult = 1 + iota
	// VoteLost indicates that the quorum has voted "no".
	VoteLost
	// VoteWon indicates that the quorum has voted "yes".
	VoteWon
)

//-------------------------------------------------------------
//------------------------lock Rand----------------------------
//-------------------------------------------------------------

// lockedRand is a small wrapper around rand.Rand to provide
// synchronization among multiple raft groups. Only the methods needed
// by the code are exposed (e.g. Intn).
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

// randomizedElectionTimeout is a random number between
// [electiontimeout, 2 * electiontimeout - 1]. It gets reset
// when raft changes its state to follower or candidate.
var globalRand = &lockedRand{
	rand: rand.New(rand.NewSource(time.Now().UnixNano())),
}

//-------------------------------------------------------------
//------------------------struct Raft--------------------------
//-------------------------------------------------------------
func (p *Raft) ResetVotes() {
	p.votes = map[uint64]bool{}
}

func (r *Raft) resetRandomizedElectionTimeout() {
	r.randomizedElectionTimeout = r.electionTimeout + globalRand.Intn(r.electionTimeout)
}

func (r *Raft) reset(term uint64) {
	if r.Term != term {
		r.Term = term
		r.Vote = None
	}
	r.Lead = None

	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.resetRandomizedElectionTimeout()

	r.ResetVotes()
}

func (r *Raft) send(m pb.Message) {
	if m.From == None {
		m.From = r.id
	}
	if m.MsgType == pb.MessageType_MsgRequestVote || m.MsgType == pb.MessageType_MsgRequestVoteResponse ||
		m.MsgType == pb.MessageType_MsgAppend || m.MsgType == pb.MessageType_MsgAppendResponse ||
		m.MsgType == pb.MessageType_MsgHeartbeat || m.MsgType == pb.MessageType_MsgHeartbeatResponse {
		if m.Term == 0 {
			panic(fmt.Sprintf("term should be set when sending %s", m.MsgType))
		}
	} else {
		if m.Term != 0 {
			panic(fmt.Sprintf("term should not be set when sending %s (was %d)", m.MsgType, m.Term))
		}
	}
	r.msgs = append(r.msgs, m)
}

func (r *Raft) hup() {
	if r.State == StateLeader {
		log.Info("[wq] %x ignoring MsgHup because already leader", r.id)
		return
	}
	log.Infof("[wq] %x is starting a new election at term %d", r.id, r.Term)
	r.becomeCandidate()
	if _, _, result := r.poll(r.id, pb.MessageType_MsgRequestVoteResponse, true); result == VoteWon {
		r.becomeLeader()
	}
	var ids []uint64
	{
		ids = make([]uint64, 0, len(r.Prs))
		for id := range r.Prs {
			ids = append(ids, id)
		}
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	}
	for _, id := range ids {
		if id == r.id {
			continue
		}
		r.send(pb.Message{
			From:    r.id,
			Term:    r.Term,
			To:      id,
			MsgType: pb.MessageType_MsgRequestVote,
			Index:   r.RaftLog.LastIndex(),
			LogTerm: r.RaftLog.LastTerm()})
	}
}

func stepLeader(r *Raft, m pb.Message) error {
	// These message types do not require any progress for m.From.
	switch m.MsgType {
	case pb.MessageType_MsgBeat:
		r.bcastHeartbeat()
		return nil
	case pb.MessageType_MsgPropose:
		if len(m.Entries) == 0 {
			log.Panicf("%x stepped empty MsgProp", r.id)
		}
		ents := make([]pb.Entry, len(m.Entries))
		for i := range ents {
			ents[i] = *m.Entries[i]
		}
		if !r.appendEntry(ents...) {
			return ErrProposalDropped
		}
		r.maybeUpdateCommit()
		r.bcastAppend()
		return nil
	}

	// All other message types require a progress for m.From (pr).
	switch m.MsgType {
	case pb.MessageType_MsgAppendResponse:
		if !m.Reject {
			if m.Index > r.Prs[m.From].Match {
				r.Prs[m.From].Match = m.Index
			}
			if m.Index+1 > r.Prs[m.From].Next {
				r.Prs[m.From].Next = m.Index + 1
			}
			if r.maybeUpdateCommit() {
				r.bcastAppend()
			}
		} else {
			if m.Index > r.Prs[m.From].Match && m.Index < r.Prs[m.From].Next {
				r.Prs[m.From].Next = m.Index
			}
			r.sendAppend(m.From)
		}
	case pb.MessageType_MsgHeartbeatResponse:
		raft_assert(!m.Reject) // 不会被拒绝，只会被忽略
		if m.Index == r.RaftLog.LastIndex() && m.Term == r.RaftLog.LastTerm() {
			// 收到心跳表示对方的log和leader至少一样新，则忽略
		} else { // 此时考虑对方比leader的日志还新,但是因为每个leader要提交一个空op，因此该情况不会出现
			log.Infof("[wq] %x has received %x %s, need to update followers logs", r.id, m.From, m.MsgType)
			r.sendAppend(m.From)
		}
	default:
		log.Infof("[wq] %x has received %s, but do nothing(maybe need be emplemented)", r.id, m.MsgType)
	}
	return nil
}

// bcastHeartbeat sends RPC, without entries to all the peers.
func (r *Raft) bcastAppend() {
	for peerId := range r.Prs {
		if peerId == r.id {
			continue
		}
		r.sendAppend(peerId)
	}
}

func stepCandidate(r *Raft, m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgRequestVoteResponse:
		gr, rj, res := r.poll(m.From, m.MsgType, !m.Reject)
		log.Infof("[wq] %x has received %d %s votes and %d vote rejections", r.id, gr, m.MsgType, rj)
		switch res {
		case VoteWon:
			r.becomeLeader()
		case VoteLost:
			r.becomeFollower(r.Term, None)
		}
	case pb.MessageType_MsgAppend:
		r.becomeFollower(m.Term, m.From)
		r.handleAppendEntries(m)
	default:
		log.Infof("[wq] %x has received %s, but do nothing(maybe need be emplemented)", r.id, m.MsgType)
	}
	return nil
}

func stepFollower(r *Raft, m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgAppend:
		r.becomeFollower(m.Term, m.From)
		r.handleAppendEntries(m)
	case pb.MessageType_MsgHeartbeat:
		raft_assert((r.Vote == m.From || r.Vote == None))
		r.becomeFollower(m.Term, m.From)
		r.handleHeartbeat(m)

	default:
		log.Infof("[wq] %x has received %s from %x, but do nothing(maybe need be emplemented)", r.id, m.MsgType, m.From)
	}
	return nil
}

// bcastHeartbeat sends RPC, without entries to all the peers.
func (r *Raft) bcastHeartbeat() {
	for peerId := range r.Prs {
		if peerId == r.id {
			continue
		}
		r.sendHeartbeat(peerId)
	}
}

func (r *Raft) poll(id uint64, t pb.MessageType, isVote bool) (granted int, rejected int, result VoteResult) {
	if isVote {
		log.Infof("%x received %s supportion from %x at term %d", r.id, t, id, r.Term)
	} else {
		log.Infof("%x received %s rejection from %x at term %d", r.id, t, id, r.Term)
	}
	r.votes[id] = isVote
	for _, vote := range r.votes {
		if vote {
			granted++
		} else {
			rejected++
		}
	}
	nPeers := len(r.Prs)
	if granted > nPeers/2 {
		result = VoteWon
	} else if nPeers-rejected > nPeers/2 {
		result = VotePending
	} else {
		result = VoteLost
	}
	return granted, rejected, result
}

func (r *Raft) appendEntry(es ...pb.Entry) (accepted bool) {
	if r.State != StateLeader {
		return false
	}
	li := r.RaftLog.LastIndex()
	for i := range es {
		es[i].Term = r.Term
		es[i].Index = li + 1 + uint64(i)
	}
	r.RaftLog.append(es...)
	return true
}

// 按理说leader是没每次受到appendresp更新
// 但是考虑只有一个节点的情况，因此，再每次propose之后日志落盘之前，也要调用该函数更新
func (r *Raft) maybeUpdateCommit() bool {
	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.Prs[r.id].Next = r.Prs[r.id].Match + 1

	var matchs []uint64
	{
		matchs = make([]uint64, 0, len(r.Prs))
		for id := range r.Prs {
			matchs = append(matchs, r.Prs[id].Match)
		}
		sort.Slice(matchs, func(i, j int) bool { return matchs[i] > matchs[j] })
	}
	majorityMatch := matchs[len(matchs)/2]
	term, err := r.RaftLog.Term(majorityMatch)

	raft_assert(err == nil)
	if majorityMatch > r.RaftLog.committed && term == r.Term {
		// raft paper: Figure 8 : only commit current log
		r.RaftLog.committed = majorityMatch
		return true
	}
	return false
}
