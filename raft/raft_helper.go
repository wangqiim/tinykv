// author: wangqi
// 该文件的目的
// 一是提供一些封装的方法和工具
// 二是将添加的方法和原方法通过文件隔离开，防止修改时影响到原因的测试

package raft

import (
	"errors"

	"math/rand"

	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

var ValidId uint64 = 0

//------------------error & assert-------------------------------------
var ErrMsgTermTooLow = errors.New("message term to low")
var ErrDefensiveAssert = errors.New("raft: defensive programming error")
var ErrNotEmplement = errors.New("something not emplement")

func raft_assert(stmt bool) {
	if !stmt {
		panic(ErrDefensiveAssert.Error())
	}
}

//-------------------------------------------------------------
//------------------------struct Raft---------------------------------
//-------------------------------------------------------------

//------------------------Raft:send RPC---------------------------------
// sendrequestVoteReq sends a requestVote RPC to the given peer.
func (r *Raft) sendrequestVoteReq(to uint64) {
	r.msgs = append(r.msgs, pb.Message{
		From:    r.id,
		To:      to,
		Term:    r.Term,
		MsgType: pb.MessageType_MsgRequestVote,
	})
}

func (r *Raft) sendrequestVoteResp(to uint64, reject bool) {
	r.msgs = append(r.msgs, pb.Message{
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Reject:  reject,
		MsgType: pb.MessageType_MsgRequestVoteResponse,
	})
}

func (r *Raft) sendMsgHeartbeatResponse(to uint64, reject bool) {
	r.msgs = append(r.msgs, pb.Message{
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Reject:  reject,
		MsgType: pb.MessageType_MsgRequestVoteResponse,
	})
}

func (r *Raft) triggerbroadcast() {
	raft_assert(r.State == StateLeader)
	for peerId := range r.Prs { // broadcast
		r.sendHeartbeat(peerId)
	}
	r.heartbeatElapsed = 0
}

//------------------------Raft:RPC helper------------------------------
func (r *Raft) checkMoreThanHalfVotes() bool {
	votes := 0
	for _, isVote := range r.votes {
		if isVote {
			votes++
		}
	}
	return votes > len(r.votes)/2
}

// handleRequestVote handle AppendEntries RPC request
func (r *Raft) progressRequestVoteResp(m *pb.Message) {
	// 1.check something
	raft_assert(m.From != r.id && m.To == r.id && m.MsgType == pb.MessageType_MsgRequestVoteResponse)
	if m.Term != r.Term || r.State == StateFollower {
		// ignore old resp for request vote that this node sended
		return
	}
	// 2. what should i do, if there many reject
	if !m.Reject {
		r.votes[m.From] = true
		if r.checkMoreThanHalfVotes() && r.State == StateCandidate {
			r.becomeLeader()
		}
	}
}

func (r *Raft) progressMsgAppend(m *pb.Message) {
	if m.Term < r.Term {
		log.Info(ErrMsgTermTooLow.Error())
	} else if m.Term >= r.Term {
		r.becomeFollower(m.Term, m.From)
	}
	// todo(wq): reply
}

func (r *Raft) progressMsgHeartbeat(m *pb.Message) {
	reject := true
	if m.Term < r.Term {
		log.Error(ErrMsgTermTooLow.Error())
	} else if m.Term >= r.Term {
		reject = false
		r.becomeFollower(m.Term, m.From)
	}
	r.sendMsgHeartbeatResponse(m.From, reject)
}

func (r *Raft) progressMsgHeartbeatResp(m *pb.Message) {
	if m.Term > r.Term {
		r.becomeFollower(m.Term, ValidId)
	}
}

func (r *Raft) progressMsgHup(m *pb.Message) {
	raft_assert(r.State != StateLeader)
	r.clearVotes()

	r.becomeCandidate()
	for peerId := range r.Prs {
		r.sendrequestVoteReq(peerId)
	}
	if r.checkMoreThanHalfVotes() {
		r.becomeLeader()
	}
}

func (r *Raft) progressMsgRequestVote(m *pb.Message) {
	reject := true
	if m.Term > r.Term {
		r.becomeFollower(m.Term, m.From)
		reject = false
	} else if m.Term == r.Term {
		if r.Vote == ValidId {
			r.becomeFollower(m.Term, m.From)
			reject = false
		} else if r.Vote == m.From { // candidate may loss a request resp, so resend a request vote
			reject = false
		}
	}
	r.sendrequestVoteResp(m.From, reject)
}

func (r *Raft) progressMsgPropose(m *pb.Message) {
	raft_assert(r.State == StateLeader && m.Entries != nil)
	// todo(wangqi): implement me
	log.Info(ErrNotEmplement.Error())
}

//--------------------raft-other-helper-----------------

// call this function when "after start to election" & "success election"
func (r *Raft) clearVotes() {
	for peerId := range r.votes {
		r.votes[peerId] = false
	}
}

// that election timeout for follower or candidate is randomized.
func (r *Raft) resetElectionTimer() {
	r.electionTimeout = r.conf.ElectionTick + rand.Intn(r.conf.ElectionTick)
	r.electionElapsed = 0
}
