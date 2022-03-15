// author: wangqi
// 该文件的目的
// 一是提供一些封装的方法和工具
// 二是将添加的方法和原方法通过文件隔离开，防止修改时影响到原因的测试

package raft

import (
	"errors"
	"log"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

var ValidId uint64 = 0

//------------------error & assert-------------------------------------
var ErrMsgTermTooLow = errors.New("message term to low")
var ErrDefensiveAssert = errors.New("raft: defensive programming error")
var ErrNotEmplement = errors.New("something not emplement")

func raft_assert(stmt bool) {
	if !stmt {
		log.Fatal(ErrDefensiveAssert.Error())
	}
}

//------------------------Raft---------------------------------
// sendrequestVote sends a requestVote RPC to the given peer.
func (r *Raft) sendrequestVote(to uint64) {
	// Your Code Here (2A).
	r.msgs = append(r.msgs, pb.Message{
		From:    r.id,
		To:      to,
		Term:    r.Term,
		MsgType: pb.MessageType_MsgRequestVote,
	})
}
