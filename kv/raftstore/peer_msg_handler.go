package raftstore

import (
	"fmt"
	"reflect"
	"time"

	"github.com/Connor1996/badger"
	"github.com/Connor1996/badger/y"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/runner"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/snap"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	errorpb "github.com/pingcap-incubator/tinykv/proto/pkg/errorpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/btree"
	"github.com/pingcap/errors"
)

type PeerTick int

const (
	PeerTickRaft               PeerTick = 0
	PeerTickRaftLogGC          PeerTick = 1
	PeerTickSplitRegionCheck   PeerTick = 2
	PeerTickSchedulerHeartbeat PeerTick = 3
)

type peerMsgHandler struct {
	*peer
	ctx *GlobalContext
}

func newPeerMsgHandler(peer *peer, ctx *GlobalContext) *peerMsgHandler {
	return &peerMsgHandler{
		peer: peer,
		ctx:  ctx,
	}
}

func (d *peerMsgHandler) HandleRaftReady() {
	if d.stopped {
		return
	}
	// Your Code Here (2B).
	if !d.RaftGroup.HasReady() {
		return
	}
	ready := d.RaftGroup.Ready()
	// 1. save hardstate and append log
	result, err := d.peerStorage.SaveReadyState(&ready)
	if err != nil {
		panic("[wq] implement me")
	}
	if result != nil && !reflect.DeepEqual(result.PrevRegion, result.Region) {
		d.ctx.storeMeta.Lock()
		defer d.ctx.storeMeta.Unlock()
		d.ctx.storeMeta.setRegion(result.Region, d.peer)
		d.ctx.storeMeta.regionRanges.Delete(&regionItem{region: result.PrevRegion})
		d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: result.Region})
		// d.peerStorage.SetRegion(result.Region)
	}
	// 2. send msg
	if len(ready.Messages) != 0 {
		d.Send(d.ctx.trans, ready.Messages)
	}
	// 3. apply ents
	if len(ready.CommittedEntries) != 0 {
		kvWB := new(engine_util.WriteBatch)
		for _, entry := range ready.CommittedEntries {
			d.process(&entry, kvWB)
			if d.stopped { // 防止连续两条 remove itself
				kvWB.DeleteMeta(meta.ApplyStateKey(d.regionId)) // 测试样例测出来的
				kvWB.MustWriteToDB(d.peerStorage.Engines.Kv)
				break
			}
		}
		if !d.stopped {
			d.peerStorage.applyState.AppliedIndex = ready.CommittedEntries[len(ready.CommittedEntries)-1].Index
			kvWB.MustSetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
			kvWB.MustWriteToDB(d.peerStorage.Engines.Kv)
		}
	}
	d.RaftGroup.Advance(ready)
}

// apply entry and maybe trigger call back
func (d *peerMsgHandler) process(entry *eraftpb.Entry, kvWB *engine_util.WriteBatch) {
	if entry.EntryType == eraftpb.EntryType_EntryConfChange {
		d.processConfChange(entry, kvWB)
		return
	}

	msg := &raft_cmdpb.RaftCmdRequest{}

	if err := msg.Unmarshal(entry.Data); err != nil {
		panic(err)
	}

	resp := &raft_cmdpb.RaftCmdResponse{Header: &raft_cmdpb.RaftResponseHeader{}}
	var txn *badger.Txn = nil

	// 1. process AdminRequest
	if msg.AdminRequest != nil {
		switch msg.AdminRequest.CmdType {
		case raft_cmdpb.AdminCmdType_CompactLog:
			if msg.AdminRequest.CompactLog.GetCompactIndex() > d.peerStorage.applyState.TruncatedState.Index {
				d.peerStorage.applyState.TruncatedState.Index = msg.AdminRequest.CompactLog.GetCompactIndex()
				d.peerStorage.applyState.TruncatedState.Term = msg.AdminRequest.CompactLog.GetCompactTerm()
				// 因为快照的truncateindex一定先于applyindex，参考 func onRaftGCLogTick()
				// 所以延迟applyState落盘(调用 process的函数来做)应该没事,assert防御一下
				d.ScheduleCompactLog(d.peerStorage.applyState.TruncatedState.Index)
			}
			return
		case raft_cmdpb.AdminCmdType_TransferLeader:
			d.RaftGroup.TransferLeader(msg.AdminRequest.TransferLeader.Peer.Id)
			resp.AdminResponse = &raft_cmdpb.AdminResponse{
				CmdType:        raft_cmdpb.AdminCmdType_TransferLeader,
				TransferLeader: &raft_cmdpb.TransferLeaderResponse{}}
		case raft_cmdpb.AdminCmdType_Split:
			d.processSplit(msg, kvWB)
			return
		case raft_cmdpb.AdminCmdType_ChangePeer:
			panic("[wq] ChangPeer 请求都是用 ConfChange发出的,不会走到该分支上,除非调整rawnode的接口,但是无法保证后面的测试是否会测这个接口,因此这个时候重构并不安全")
		default:
			panic("[wq] implement me")
		}
	} else {
		// 2. add write batch
		for _, req := range msg.Requests {
			if err := checkKey(getRequestKey(req), d.Region()); err != nil {
				resp.Header.Error = &errorpb.Error{
					Message:        "Key not in region",
					KeyNotInRegion: &errorpb.KeyNotInRegion{Key: getRequestKey(req), RegionId: d.regionId},
				}
				break
			}
			switch req.CmdType {
			case raft_cmdpb.CmdType_Invalid:
				panic("[wq] implement me")
			case raft_cmdpb.CmdType_Get:
				value, err := engine_util.GetCF(d.peerStorage.Engines.Kv, req.Get.Cf, req.Get.Key)
				if err != nil {
					log.Warnf("[wq] get no key! err= %s", err.Error())
				}
				resp.Responses = append(resp.Responses, &raft_cmdpb.Response{
					CmdType: raft_cmdpb.CmdType_Get,
					Get:     &raft_cmdpb.GetResponse{Value: value}})
			case raft_cmdpb.CmdType_Put:
				kvWB.SetCF(req.Put.Cf, req.Put.Key, req.Put.Value)
				// log.Infof("[wq] [Tag %s] write key %v, value %v",
				// 	d.Tag, string(req.Put.Key), string(req.Put.Value))
				resp.Responses = append(resp.Responses, &raft_cmdpb.Response{
					CmdType: raft_cmdpb.CmdType_Put,
					Put:     &raft_cmdpb.PutResponse{}})
			case raft_cmdpb.CmdType_Delete:
				kvWB.DeleteCF(req.Delete.Cf, req.Delete.Key)
				resp.Responses = append(resp.Responses, &raft_cmdpb.Response{
					CmdType: raft_cmdpb.CmdType_Delete,
					Delete:  &raft_cmdpb.DeleteResponse{}})
			case raft_cmdpb.CmdType_Snap:
				if err := util.CheckRegionEpoch(msg, d.Region(), true); err != nil {
					resp.Header.Error = &errorpb.Error{
						Message:       "Epoch Not Match",
						EpochNotMatch: &errorpb.EpochNotMatch{},
					}
					break
				}
				resp.Responses = append(resp.Responses, &raft_cmdpb.Response{
					CmdType: raft_cmdpb.CmdType_Snap,
					Snap: &raft_cmdpb.SnapResponse{
						Region: d.Region(), // 面向panic样例编程
					}})
				txn = d.peerStorage.Engines.Kv.NewTransaction(false) // 只读事务
			}
		}
	}
	d.processCallback(entry, resp, txn)
}

// 这个confchange有点弱智，测试样例好像需要单独构造一个和其他命令不同的req，因此单独提出来
func (d *peerMsgHandler) processConfChange(entry *eraftpb.Entry, kvWB *engine_util.WriteBatch) {
	cc := &eraftpb.ConfChange{}
	err := cc.Unmarshal(entry.Data)
	y.Assert(err == nil)
	req := &raft_cmdpb.ChangePeerRequest{}
	err = req.Unmarshal(cc.Context)
	y.Assert(err == nil)
	switch cc.ChangeType {
	case eraftpb.ConfChangeType_AddNode:
		if regionContainPeer(d.Region(), req.Peer.GetId()) {
			// log.Infof("[wq] ignore addNode, Region.ConfVer: %v", d.Region().RegionEpoch.ConfVer)
			return
		}
		d.Region().RegionEpoch.ConfVer++
		log.Infof("[wq] Node %s, add: %d Region.ConfVer: %d", d.Tag, req.Peer.GetId(), d.Region().RegionEpoch.ConfVer)
		d.Region().Peers = append(d.Region().Peers, &metapb.Peer{Id: req.Peer.GetId(), StoreId: req.Peer.GetStoreId()})
		meta.WriteRegionState(kvWB, d.Region(), raft_serverpb.PeerState_Normal)
	case eraftpb.ConfChangeType_RemoveNode:
		if !regionContainPeer(d.Region(), req.Peer.GetId()) {
			// log.Infof("[wq] ignore removeNode, Region.ConfVer: %v", d.Region().RegionEpoch.ConfVer)
			return
		}
		if req.Peer.GetId() == d.PeerId() {
			log.Infof("[wq] apply remove self Node %s, Region: %s", d.Tag, d.Region().String())
			d.destroyPeer()
			kvWB.DeleteMeta(meta.RegionStateKey(req.Peer.GetId()))
			return
		}
		d.Region().RegionEpoch.ConfVer++
		log.Infof("[wq] Node %s, remove: %d Region.ConfVer: %d", d.Tag, req.Peer.GetId(), d.Region().RegionEpoch.ConfVer)
		for i, peer := range d.Region().Peers {
			if peer.Id == req.Peer.GetId() {
				d.Region().Peers = append(d.Region().Peers[:i], d.Region().Peers[i+1:]...)
				break
			}
		}
		meta.WriteRegionState(kvWB, d.Region(), raft_serverpb.PeerState_Normal)
		// y.Assert(cc.NodeId == req.Peer.GetId())
		d.removePeerCache(cc.NodeId)
	}
	confState := d.RaftGroup.ApplyConfChange(*cc)
	log.Infof("[wq] %s applyconfchange %s, nodes = %v", d.Tag, cc.ChangeType.String(), confState.Nodes)
	_ = confState // useless
	resp := &raft_cmdpb.RaftCmdResponse{
		Header: &raft_cmdpb.RaftResponseHeader{},
		AdminResponse: &raft_cmdpb.AdminResponse{
			CmdType:    raft_cmdpb.AdminCmdType_ChangePeer,
			ChangePeer: &raft_cmdpb.ChangePeerResponse{},
		},
	}
	d.processCallback(entry, resp, nil)
}

func (d *peerMsgHandler) processSplit(msg *raft_cmdpb.RaftCmdRequest, wb *engine_util.WriteBatch) error {
	splitReq := msg.GetAdminRequest().GetSplit()
	if err := checkKeyAndEpoch(splitReq.SplitKey, msg, d.Region()); err != nil {
		return err
	}

	y.Assert(len(splitReq.GetNewPeerIds()) == len(d.Region().GetPeers()))
	newPeers := make([]*metapb.Peer, len(d.Region().GetPeers()))
	for i, peer := range d.Region().GetPeers() {
		newPeers[i] = &metapb.Peer{
			Id:      splitReq.NewPeerIds[i],
			StoreId: peer.StoreId,
		}
	}
	newRegion := &metapb.Region{
		Id:       splitReq.NewRegionId,
		StartKey: splitReq.SplitKey,
		EndKey:   d.Region().EndKey,
		RegionEpoch: &metapb.RegionEpoch{
			ConfVer: 1,
			Version: 1,
		},
		Peers: newPeers,
	}
	newPeer, err := createPeer(d.storeID(), d.ctx.cfg, d.ctx.regionTaskSender, d.ctx.engine, newRegion)
	y.Assert(err == nil)

	sm := d.ctx.storeMeta
	sm.Lock()
	d.Region().RegionEpoch.Version += 1
	d.Region().EndKey = splitReq.SplitKey
	sm.regionRanges.ReplaceOrInsert(&regionItem{region: d.Region()})
	sm.regionRanges.ReplaceOrInsert(&regionItem{region: newRegion})
	sm.regions[splitReq.NewRegionId] = newRegion
	sm.Unlock()

	meta.WriteRegionState(wb, d.Region(), rspb.PeerState_Normal)
	meta.WriteRegionState(wb, newRegion, rspb.PeerState_Normal)

	d.ctx.router.register(newPeer)
	err = d.ctx.router.send(newRegion.Id, message.NewMsg(message.MsgTypeStart, nil))
	y.Assert(err == nil)

	return nil
}

func (d *peerMsgHandler) HandleMsg(msg message.Msg) {
	switch msg.Type {
	case message.MsgTypeRaftMessage:
		raftMsg := msg.Data.(*rspb.RaftMessage)
		if err := d.onRaftMsg(raftMsg); err != nil {
			log.Errorf("%s handle raft message error %v", d.Tag, err)
		}
	case message.MsgTypeRaftCmd:
		raftCMD := msg.Data.(*message.MsgRaftCmd)
		d.proposeRaftCommand(raftCMD.Request, raftCMD.Callback)
	case message.MsgTypeTick:
		d.onTick()
	case message.MsgTypeSplitRegion:
		split := msg.Data.(*message.MsgSplitRegion)
		log.Infof("%s on split with %v", d.Tag, split.SplitKey)
		d.onPrepareSplitRegion(split.RegionEpoch, split.SplitKey, split.Callback)
	case message.MsgTypeRegionApproximateSize:
		d.onApproximateRegionSize(msg.Data.(uint64))
	case message.MsgTypeGcSnap:
		gcSnap := msg.Data.(*message.MsgGCSnap)
		d.onGCSnap(gcSnap.Snaps)
	case message.MsgTypeStart:
		d.startTicker()
	}
}

func (d *peerMsgHandler) preProposeRaftCommand(req *raft_cmdpb.RaftCmdRequest) error {
	// Check store_id, make sure that the msg is dispatched to the right place.
	if err := util.CheckStoreID(req, d.storeID()); err != nil {
		return err
	}

	// Check whether the store has the right peer to handle the request.
	regionID := d.regionId
	leaderID := d.LeaderId()
	if !d.IsLeader() {
		leader := d.getPeerFromCache(leaderID)
		return &util.ErrNotLeader{RegionId: regionID, Leader: leader}
	}
	// peer_id must be the same as peer's.
	if err := util.CheckPeerID(req, d.PeerId()); err != nil {
		return err
	}
	// Check whether the term is stale.
	if err := util.CheckTerm(req, d.Term()); err != nil {
		return err
	}
	err := util.CheckRegionEpoch(req, d.Region(), true)
	if errEpochNotMatching, ok := err.(*util.ErrEpochNotMatch); ok {
		// Attach the region which might be split from the current region. But it doesn't
		// matter if the region is not split from the current region. If the region meta
		// received by the TiKV driver is newer than the meta cached in the driver, the meta is
		// updated.
		siblingRegion := d.findSiblingRegion()
		if siblingRegion != nil {
			errEpochNotMatching.Regions = append(errEpochNotMatching.Regions, siblingRegion)
		}
		return errEpochNotMatching
	}
	return err
}

func (d *peerMsgHandler) proposeRaftCommand(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	err := d.preProposeRaftCommand(msg)
	if err != nil {
		cb.Done(ErrResp(err))
		return
	}
	// log.Infof("[wq] [Tag %s] [leader: %v, Vote: %v Term: %v] propose raft command %v",
	// 	d.Tag, d.RaftGroup.Raft.Lead, d.RaftGroup.Raft.Vote, d.RaftGroup.Raft.Term, d.nextProposalIndex())
	if msg.AdminRequest != nil {
		switch msg.AdminRequest.CmdType {
		case raft_cmdpb.AdminCmdType_CompactLog, raft_cmdpb.AdminCmdType_TransferLeader:
			data, err := msg.Marshal()
			y.Assert(err == nil)
			p := &proposal{index: d.nextProposalIndex(), term: d.Term(), cb: cb}
			d.proposals = append(d.proposals, p)
			if err = d.RaftGroup.Propose(data); err != nil {
				log.Infof("[wq] propose err: %s, msg: %s", err.Error(), msg.String()) // maybe drop propose, because transferring or has been not leader
				// cb.Done(ErrResp(err)) don't reply error to transfer req
				return
			}
		case raft_cmdpb.AdminCmdType_ChangePeer:
			if msg.AdminRequest.ChangePeer.ChangeType == eraftpb.ConfChangeType_RemoveNode &&
				d.RaftGroup.Raft.Lead == msg.AdminRequest.ChangePeer.Peer.Id {
				y.Assert(d.RaftGroup.Raft.Lead == d.RaftGroup.Raft.GetId()) // 自己应该是leader
				// 直接拒绝, 转移leader, 可以一定程度上避免bug https://asktug.com/t/topic/274196
				for peerId := range d.RaftGroup.Raft.Prs {
					if peerId != d.RaftGroup.Raft.GetId() {
						d.RaftGroup.TransferLeader(peerId)
						break
					}
				}
				cb.Done(&raft_cmdpb.RaftCmdResponse{
					Header: &raft_cmdpb.RaftResponseHeader{},
					AdminResponse: &raft_cmdpb.AdminResponse{
						CmdType:    raft_cmdpb.AdminCmdType_ChangePeer,
						ChangePeer: &raft_cmdpb.ChangePeerResponse{},
					},
				})
				return
			}
			ctx, err := msg.AdminRequest.ChangePeer.Marshal()
			y.Assert(err == nil)
			cc := eraftpb.ConfChange{
				ChangeType: msg.AdminRequest.ChangePeer.ChangeType,
				NodeId:     msg.AdminRequest.ChangePeer.Peer.Id,
				Context:    ctx}
			p := &proposal{index: d.nextProposalIndex(), term: d.Term(), cb: cb}
			d.proposals = append(d.proposals, p)
			err = d.RaftGroup.ProposeConfChange(cc)
			y.Assert(err == nil)
			log.Infof("[wq] propose ChangePeer %s,  %d", eraftpb.ConfChangeType_name[int32(cc.ChangeType)], cc.NodeId)
			return // confchange 比较特殊(由于raftnode有单独定义的接口)
		case raft_cmdpb.AdminCmdType_Split:
			if err = checkKeyAndEpoch(msg.AdminRequest.Split.SplitKey, msg, d.Region()); err != nil {
				return
			}
			data, err := msg.Marshal()
			y.Assert(err == nil)
			y.Assert(cb == nil) // don't need callback
			// p := &proposal{index: d.nextProposalIndex(), term: d.Term(), cb: cb}
			// d.proposals = append(d.proposals, p)
			if err = d.RaftGroup.Propose(data); err != nil {
				log.Infof("[wq] propose err: %s, msg: %s", err.Error(), msg.String()) // maybe drop propose, because transferring or has been not leader
				return
			}
		default:
			log.Panicf("[wq] not implement %s", msg.AdminRequest.CmdType.String())
		}
	} else {
		data, err := msg.Marshal()
		if err != nil {
			panic(err)
		}
		p := &proposal{index: d.nextProposalIndex(), term: d.Term(), cb: cb}
		d.proposals = append(d.proposals, p)

		// log.Infof("[wq] raftId: %d propose msg: %s", d.RaftGroup.Raft.GetId(), msg.String()) // maybe drop propose, because transferring or has been not leader
		if err = d.RaftGroup.Propose(data); err != nil {
			// log.Infof("[wq] propose err: %s, msg: %s", err.Error(), msg.String()) // maybe drop propose, because transferring or has been not leader
		}
	}
}

func (d *peerMsgHandler) onTick() {
	if d.stopped {
		return
	}
	d.ticker.tickClock()
	if d.ticker.isOnTick(PeerTickRaft) {
		d.onRaftBaseTick()
	}
	if d.ticker.isOnTick(PeerTickRaftLogGC) {
		d.onRaftGCLogTick()
	}
	if d.ticker.isOnTick(PeerTickSchedulerHeartbeat) {
		d.onSchedulerHeartbeatTick()
	}
	if d.ticker.isOnTick(PeerTickSplitRegionCheck) {
		d.onSplitRegionCheckTick()
	}
	d.ctx.tickDriverSender <- d.regionId
}

func (d *peerMsgHandler) startTicker() {
	d.ticker = newTicker(d.regionId, d.ctx.cfg)
	d.ctx.tickDriverSender <- d.regionId
	d.ticker.schedule(PeerTickRaft)
	d.ticker.schedule(PeerTickRaftLogGC)
	d.ticker.schedule(PeerTickSplitRegionCheck)
	d.ticker.schedule(PeerTickSchedulerHeartbeat)
}

func (d *peerMsgHandler) onRaftBaseTick() {
	d.RaftGroup.Tick()
	d.ticker.schedule(PeerTickRaft)
}

func (d *peerMsgHandler) ScheduleCompactLog(truncatedIndex uint64) {
	raftLogGCTask := &runner.RaftLogGCTask{
		RaftEngine: d.ctx.engine.Raft,
		RegionID:   d.regionId,
		StartIdx:   d.LastCompactedIdx,
		EndIdx:     truncatedIndex + 1,
	}
	d.LastCompactedIdx = raftLogGCTask.EndIdx
	d.ctx.raftLogGCTaskSender <- raftLogGCTask
}

func (d *peerMsgHandler) onRaftMsg(msg *rspb.RaftMessage) error {
	// log.Infof("%s handle raft message %s from %d to %d",
	// 	d.Tag, msg.GetMessage().GetMsgType(), msg.GetFromPeer().GetId(), msg.GetToPeer().GetId())
	if !d.validateRaftMessage(msg) {
		return nil
	}
	if d.stopped {
		return nil
	}
	if msg.GetIsTombstone() {
		// we receive a message tells us to remove self.
		d.handleGCPeerMsg(msg)
		return nil
	}
	if d.checkMessage(msg) {
		return nil
	}
	key, err := d.checkSnapshot(msg)
	if err != nil {
		return err
	}
	if key != nil {
		// If the snapshot file is not used again, then it's OK to
		// delete them here. If the snapshot file will be reused when
		// receiving, then it will fail to pass the check again, so
		// missing snapshot files should not be noticed.
		s, err1 := d.ctx.snapMgr.GetSnapshotForApplying(*key)
		if err1 != nil {
			return err1
		}
		d.ctx.snapMgr.DeleteSnapshot(*key, s, false)
		return nil
	}
	d.insertPeerCache(msg.GetFromPeer())
	err = d.RaftGroup.Step(*msg.GetMessage())
	if err != nil {
		return err
	}
	if d.AnyNewPeerCatchUp(msg.FromPeer.Id) {
		d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
	}
	return nil
}

// return false means the message is invalid, and can be ignored.
func (d *peerMsgHandler) validateRaftMessage(msg *rspb.RaftMessage) bool {
	regionID := msg.GetRegionId()
	from := msg.GetFromPeer()
	to := msg.GetToPeer()
	log.Debugf("[region %d] handle raft message %s from %d to %d", regionID, msg, from.GetId(), to.GetId())
	if to.GetStoreId() != d.storeID() {
		log.Warnf("[region %d] store not match, to store id %d, mine %d, ignore it",
			regionID, to.GetStoreId(), d.storeID())
		return false
	}
	if msg.RegionEpoch == nil {
		log.Errorf("[region %d] missing epoch in raft message, ignore it", regionID)
		return false
	}
	return true
}

/// Checks if the message is sent to the correct peer.
///
/// Returns true means that the message can be dropped silently.
func (d *peerMsgHandler) checkMessage(msg *rspb.RaftMessage) bool {
	fromEpoch := msg.GetRegionEpoch()
	isVoteMsg := util.IsVoteMessage(msg.Message)
	fromStoreID := msg.FromPeer.GetStoreId()

	// Let's consider following cases with three nodes [1, 2, 3] and 1 is leader:
	// a. 1 removes 2, 2 may still send MsgAppendResponse to 1.
	//  We should ignore this stale message and let 2 remove itself after
	//  applying the ConfChange log.
	// b. 2 is isolated, 1 removes 2. When 2 rejoins the cluster, 2 will
	//  send stale MsgRequestVote to 1 and 3, at this time, we should tell 2 to gc itself.
	// c. 2 is isolated but can communicate with 3. 1 removes 3.
	//  2 will send stale MsgRequestVote to 3, 3 should ignore this message.
	// d. 2 is isolated but can communicate with 3. 1 removes 2, then adds 4, remove 3.
	//  2 will send stale MsgRequestVote to 3, 3 should tell 2 to gc itself.
	// e. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader.
	//  After 2 rejoins the cluster, 2 may send stale MsgRequestVote to 1 and 3,
	//  1 and 3 will ignore this message. Later 4 will send messages to 2 and 2 will
	//  rejoin the raft group again.
	// f. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader, and 4 removes 2.
	//  unlike case e, 2 will be stale forever.
	// TODO: for case f, if 2 is stale for a long time, 2 will communicate with scheduler and scheduler will
	// tell 2 is stale, so 2 can remove itself.
	region := d.Region()
	if util.IsEpochStale(fromEpoch, region.RegionEpoch) && util.FindPeer(region, fromStoreID) == nil {
		// The message is stale and not in current region.
		handleStaleMsg(d.ctx.trans, msg, region.RegionEpoch, isVoteMsg)
		return true
	}
	target := msg.GetToPeer()
	if target.Id < d.PeerId() {
		log.Infof("%s target peer ID %d is less than %d, msg maybe stale", d.Tag, target.Id, d.PeerId())
		return true
	} else if target.Id > d.PeerId() {
		if d.MaybeDestroy() {
			log.Infof("%s is stale as received a larger peer %s, destroying", d.Tag, target)
			d.destroyPeer()
			d.ctx.router.sendStore(message.NewMsg(message.MsgTypeStoreRaftMessage, msg))
		}
		return true
	}
	return false
}

func handleStaleMsg(trans Transport, msg *rspb.RaftMessage, curEpoch *metapb.RegionEpoch,
	needGC bool) {
	regionID := msg.RegionId
	fromPeer := msg.FromPeer
	toPeer := msg.ToPeer
	// msgType := msg.Message.GetMsgType()

	if !needGC {
		log.Infof("[region %d] raft message %s is stale, current %v ignore it",
			regionID, msg.String(), curEpoch)
		return
	}
	gcMsg := &rspb.RaftMessage{
		RegionId:    regionID,
		FromPeer:    toPeer,
		ToPeer:      fromPeer,
		RegionEpoch: curEpoch,
		IsTombstone: true,
	}
	if err := trans.Send(gcMsg); err != nil {
		log.Errorf("[region %d] send message failed %v", regionID, err)
	}
}

func (d *peerMsgHandler) handleGCPeerMsg(msg *rspb.RaftMessage) {
	fromEpoch := msg.RegionEpoch
	if !util.IsEpochStale(d.Region().RegionEpoch, fromEpoch) {
		return
	}
	if !util.PeerEqual(d.Meta, msg.ToPeer) {
		log.Infof("%s receive stale gc msg, ignore", d.Tag)
		return
	}
	log.Infof("%s peer %s receives gc message, trying to remove", d.Tag, msg.ToPeer)
	if d.MaybeDestroy() {
		d.destroyPeer()
	}
}

// Returns `None` if the `msg` doesn't contain a snapshot or it contains a snapshot which
// doesn't conflict with any other snapshots or regions. Otherwise a `snap.SnapKey` is returned.
func (d *peerMsgHandler) checkSnapshot(msg *rspb.RaftMessage) (*snap.SnapKey, error) {
	if msg.Message.Snapshot == nil {
		return nil, nil
	}
	regionID := msg.RegionId
	snapshot := msg.Message.Snapshot
	key := snap.SnapKeyFromRegionSnap(regionID, snapshot)
	snapData := new(rspb.RaftSnapshotData)
	err := snapData.Unmarshal(snapshot.Data)
	if err != nil {
		return nil, err
	}
	snapRegion := snapData.Region
	peerID := msg.ToPeer.Id
	var contains bool
	for _, peer := range snapRegion.Peers {
		if peer.Id == peerID {
			contains = true
			break
		}
	}
	if !contains {
		log.Infof("%s %s doesn't contains peer %d, skip", d.Tag, snapRegion, peerID)
		return &key, nil
	}
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	if !util.RegionEqual(meta.regions[d.regionId], d.Region()) {
		if !d.isInitialized() {
			log.Infof("%s stale delegate detected, skip", d.Tag)
			return &key, nil
		} else {
			panic(fmt.Sprintf("%s meta corrupted %s != %s", d.Tag, meta.regions[d.regionId], d.Region()))
		}
	}

	existRegions := meta.getOverlapRegions(snapRegion)
	for _, existRegion := range existRegions {
		if existRegion.GetId() == snapRegion.GetId() {
			continue
		}
		log.Infof("%s region overlapped %s %s", d.Tag, existRegion, snapRegion)
		return &key, nil
	}

	// check if snapshot file exists.
	_, err = d.ctx.snapMgr.GetSnapshotForApplying(key)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (d *peerMsgHandler) destroyPeer() {
	log.Infof("%s starts destroy", d.Tag)
	regionID := d.regionId
	// We can't destroy a peer which is applying snapshot.
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	isInitialized := d.isInitialized()
	if err := d.Destroy(d.ctx.engine, false); err != nil {
		// If not panic here, the peer will be recreated in the next restart,
		// then it will be gc again. But if some overlap region is created
		// before restarting, the gc action will delete the overlap region's
		// data too.
		panic(fmt.Sprintf("%s destroy peer %v", d.Tag, err))
	}
	d.ctx.router.close(regionID)
	d.stopped = true
	if isInitialized && meta.regionRanges.Delete(&regionItem{region: d.Region()}) == nil {
		panic(d.Tag + " meta corruption detected")
	}
	if _, ok := meta.regions[regionID]; !ok {
		panic(d.Tag + " meta corruption detected")
	}
	delete(meta.regions, regionID)
}

func (d *peerMsgHandler) findSiblingRegion() (result *metapb.Region) {
	meta := d.ctx.storeMeta
	meta.RLock()
	defer meta.RUnlock()
	item := &regionItem{region: d.Region()}
	meta.regionRanges.AscendGreaterOrEqual(item, func(i btree.Item) bool {
		result = i.(*regionItem).region
		return true
	})
	return
}

func (d *peerMsgHandler) onRaftGCLogTick() {
	d.ticker.schedule(PeerTickRaftLogGC)
	if !d.IsLeader() {
		return
	}

	appliedIdx := d.peerStorage.AppliedIndex()
	firstIdx, _ := d.peerStorage.FirstIndex()
	var compactIdx uint64
	if appliedIdx > firstIdx && appliedIdx-firstIdx >= d.ctx.cfg.RaftLogGcCountLimit {
		compactIdx = appliedIdx
	} else {
		return
	}

	y.Assert(compactIdx > 0)
	compactIdx -= 1
	if compactIdx < firstIdx {
		// In case compact_idx == first_idx before subtraction.
		return
	}

	term, err := d.RaftGroup.Raft.RaftLog.Term(compactIdx)
	if err != nil {
		log.Fatalf("appliedIdx: %d, firstIdx: %d, compactIdx: %d", appliedIdx, firstIdx, compactIdx)
		panic(err)
	}

	// Create a compact log request and notify directly.
	regionID := d.regionId
	request := newCompactLogRequest(regionID, d.Meta, compactIdx, term)
	d.proposeRaftCommand(request, nil)
}

func (d *peerMsgHandler) onSplitRegionCheckTick() {
	d.ticker.schedule(PeerTickSplitRegionCheck)
	// To avoid frequent scan, we only add new scan tasks if all previous tasks
	// have finished.
	if len(d.ctx.splitCheckTaskSender) > 0 {
		return
	}

	if !d.IsLeader() {
		return
	}
	if d.ApproximateSize != nil && d.SizeDiffHint < d.ctx.cfg.RegionSplitSize/8 {
		return
	}
	d.ctx.splitCheckTaskSender <- &runner.SplitCheckTask{
		Region: d.Region(),
	}
	d.SizeDiffHint = 0
}

func (d *peerMsgHandler) onPrepareSplitRegion(regionEpoch *metapb.RegionEpoch, splitKey []byte, cb *message.Callback) {
	if err := d.validateSplitRegion(regionEpoch, splitKey); err != nil {
		cb.Done(ErrResp(err))
		return
	}
	region := d.Region()
	d.ctx.schedulerTaskSender <- &runner.SchedulerAskSplitTask{
		Region:   region,
		SplitKey: splitKey,
		Peer:     d.Meta,
		Callback: cb,
	}
}

func (d *peerMsgHandler) validateSplitRegion(epoch *metapb.RegionEpoch, splitKey []byte) error {
	if len(splitKey) == 0 {
		err := errors.Errorf("%s split key should not be empty", d.Tag)
		log.Error(err)
		return err
	}

	if !d.IsLeader() {
		// region on this store is no longer leader, skipped.
		log.Infof("%s not leader, skip", d.Tag)
		return &util.ErrNotLeader{
			RegionId: d.regionId,
			Leader:   d.getPeerFromCache(d.LeaderId()),
		}
	}

	region := d.Region()
	latestEpoch := region.GetRegionEpoch()

	// This is a little difference for `check_region_epoch` in region split case.
	// Here we just need to check `version` because `conf_ver` will be update
	// to the latest value of the peer, and then send to Scheduler.
	if latestEpoch.Version != epoch.Version {
		log.Infof("%s epoch changed, retry later, prev_epoch: %s, epoch %s",
			d.Tag, latestEpoch, epoch)
		return &util.ErrEpochNotMatch{
			Message: fmt.Sprintf("%s epoch changed %s != %s, retry later", d.Tag, latestEpoch, epoch),
			Regions: []*metapb.Region{region},
		}
	}
	return nil
}

func (d *peerMsgHandler) onApproximateRegionSize(size uint64) {
	d.ApproximateSize = &size
}

func (d *peerMsgHandler) onSchedulerHeartbeatTick() {
	d.ticker.schedule(PeerTickSchedulerHeartbeat)

	if !d.IsLeader() {
		return
	}
	d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
}

func (d *peerMsgHandler) onGCSnap(snaps []snap.SnapKeyWithSending) {
	compactedIdx := d.peerStorage.truncatedIndex()
	compactedTerm := d.peerStorage.truncatedTerm()
	for _, snapKeyWithSending := range snaps {
		key := snapKeyWithSending.SnapKey
		if snapKeyWithSending.IsSending {
			snap, err := d.ctx.snapMgr.GetSnapshotForSending(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			if key.Term < compactedTerm || key.Index < compactedIdx {
				log.Infof("%s snap file %s has been compacted, delete", d.Tag, key)
				d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
			} else if fi, err1 := snap.Meta(); err1 == nil {
				modTime := fi.ModTime()
				if time.Since(modTime) > 4*time.Hour {
					log.Infof("%s snap file %s has been expired, delete", d.Tag, key)
					d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
				}
			}
		} else if key.Term <= compactedTerm &&
			(key.Index < compactedIdx || key.Index == compactedIdx) {
			log.Infof("%s snap file %s has been applied, delete", d.Tag, key)
			a, err := d.ctx.snapMgr.GetSnapshotForApplying(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			d.ctx.snapMgr.DeleteSnapshot(key, a, false)
		}
	}
}

func newAdminRequest(regionID uint64, peer *metapb.Peer) *raft_cmdpb.RaftCmdRequest {
	return &raft_cmdpb.RaftCmdRequest{
		Header: &raft_cmdpb.RaftRequestHeader{
			RegionId: regionID,
			Peer:     peer,
		},
	}
}

func newCompactLogRequest(regionID uint64, peer *metapb.Peer, compactIndex, compactTerm uint64) *raft_cmdpb.RaftCmdRequest {
	req := newAdminRequest(regionID, peer)
	req.AdminRequest = &raft_cmdpb.AdminRequest{
		CmdType: raft_cmdpb.AdminCmdType_CompactLog,
		CompactLog: &raft_cmdpb.CompactLogRequest{
			CompactIndex: compactIndex,
			CompactTerm:  compactTerm,
		},
	}
	return req
}

func regionContainPeer(region *metapb.Region, peerId uint64) bool {
	for _, peer := range region.Peers {
		if peer.Id == peerId {
			return true
		}
	}
	return false
}

// 通过entry的index和term来校对是不是需要触发的callback, 同时截断掉stale callback
func (d *peerMsgHandler) processCallback(entry *eraftpb.Entry, resp *raft_cmdpb.RaftCmdResponse, txn *badger.Txn) {
	// 1. process proposals
	var p *proposal = nil
	for len(d.proposals) > 0 && d.proposals[0].index < entry.Index {
		d.proposals = d.proposals[1:]
	}
	// 2. may process client proposal requests
	if len(d.proposals) > 0 {
		p = d.proposals[0]
		if p.index < entry.Index {
			log.Panicf("[wq] applying entry.index: %x greater than proposals[0].index: %x", entry.Index, p.index)
		} else if p.index > entry.Index {
			// log.Infof("[wq] applying entry.index: %x smaller than proposals[0].index: %x", entry.Index, p.index)
			p.cb.Done(ErrResp(&util.ErrStaleCommand{}))
		} else {
			if entry.Term != p.term {
				log.Infof("[wq] applying index: %x, entry.Term: %x is not equal to proposals[0].term: %x",
					entry.Index, entry.Term, p.term)
				p.cb.Done(ErrResp(&util.ErrStaleCommand{}))
			} else {
				p.cb.Txn = txn // snap read
				p.cb.Done(resp)
			}
		}
		d.proposals = d.proposals[1:]
	}
}

func checkKey(key []byte, region *metapb.Region) error {
	if key != nil {
		err := util.CheckKeyInRegion(key, region)
		if err != nil {
			return err
		}
	}
	return nil
}

func getRequestKey(req *raft_cmdpb.Request) []byte {
	switch req.CmdType {
	case raft_cmdpb.CmdType_Get:
		return req.Get.Key
	case raft_cmdpb.CmdType_Put:
		return req.Put.Key
	case raft_cmdpb.CmdType_Delete:
		return req.Delete.Key
	}
	return nil
}

func checkKeyAndEpoch(key []byte, req *raft_cmdpb.RaftCmdRequest, region *metapb.Region) error {
	if key != nil {
		err := util.CheckKeyInRegion(key, region)
		if err != nil {
			return err
		}
	}
	return util.CheckRegionEpoch(req, region, true)
}
