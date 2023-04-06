package raftstore

import (
	"bytes"
	"encoding/binary"
	"fmt"
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
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
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
	if d.RaftGroup.HasReady() {
		rd := d.RaftGroup.Ready()

		result, _ := d.peerStorage.SaveReadyState(&rd)
		if result != nil {
			meta := d.ctx.storeMeta
			meta.Lock()
			meta.setRegion(result.Region, d.peer)
			meta.regionRanges.ReplaceOrInsert(&regionItem{region: result.Region})
			meta.Unlock()
		}
		d.Send(d.ctx.trans, rd.Messages)

		if len(rd.CommittedEntries) > 0 {
			d.applyEntries(rd.CommittedEntries)
		}

		d.RaftGroup.Advance(rd)
	}
}

func (d *peerMsgHandler) applyEntries(entries []eraftpb.Entry) {
	for _, entry := range entries {
		if !d.stopped { //可能前面有些entry会销毁此peer，在此判断
			d.applySingleEntry(&entry)
		}
	}
}

func (d *peerMsgHandler) initNewRegion(newRegion, region *metapb.Region, splitReq *raft_cmdpb.SplitRequest) {
	newRegion.Id = splitReq.NewRegionId
	newRegion.StartKey = splitReq.SplitKey
	newRegion.EndKey = region.EndKey
	newRegion.RegionEpoch = &metapb.RegionEpoch{
		ConfVer: region.RegionEpoch.ConfVer,
		Version: region.RegionEpoch.Version,
	}

	if len(region.Peers) != len(splitReq.NewPeerIds) {
		log.Panicf("len(region.Peers) != len(splitReq.NewPeerIds)")
	}

	newPeers := make([]*metapb.Peer, 0, len(splitReq.NewPeerIds))
	for i := 0; i < len(splitReq.NewPeerIds); i++ {
		newPeers = append(newPeers, &metapb.Peer{
			Id:      splitReq.NewPeerIds[i],
			StoreId: region.Peers[i].StoreId,
		})
	}
	newRegion.Peers = newPeers
}

func (d *peerMsgHandler) applySingleEntry(entry *eraftpb.Entry) {
	if entry.EntryType == eraftpb.EntryType_EntryNormal {
		lastApplied := d.peerStorage.applyState.AppliedIndex
		if lastApplied+1 != entry.Index {
			log.Panicf("%v apply out of order last %d now %d", d.Tag, lastApplied, entry.Index)
		}

		if entry.Data == nil {
			kvWB := new(engine_util.WriteBatch)
			d.peerStorage.applyState.AppliedIndex = entry.Index
			kvWB.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
			kvWB.MustWriteToDB(d.peerStorage.Engines.Kv)
			return
		}

		var msg raft_cmdpb.RaftCmdRequest
		if err := msg.Unmarshal(entry.Data); err != nil {
			log.Panic(err)
		}
		// normal requests

		region := d.Region()
		if len(msg.Requests) > 0 {
			txn := d.peerStorage.Engines.Kv.NewTransaction(true)
			resp := newCmdResp()
			reqs := msg.Requests

			if util.IsEpochStale(msg.Header.GetRegionEpoch(), region.RegionEpoch) {
				log.Infof("%s request region epoch not match, now(%v), req(%v)", d.Tag, region.RegionEpoch, msg.Header.GetRegionEpoch())
				BindRespError(resp, &util.ErrEpochNotMatch{})
			}

			for _, req := range reqs {
				if isKeyCmd(req) {
					key := getKeyFromRequest(req)
					log.Infof("%s req key(%s) start(%s), end(%s)", d.Tag, key, region.StartKey, region.EndKey)
					if !regionContainsKey(region, key) {
						BindRespError(resp, &util.ErrKeyNotInRegion{
							Key:    key,
							Region: region,
						})
						log.Infof("%s return key not in region error", d.Tag)
					}
				}

				if resp.Header.Error == nil {
					switch req.CmdType {
					case raft_cmdpb.CmdType_Get:
						val, err := engine_util.GetCFFromTxn(txn, req.Get.Cf, req.Get.Key)
						if err != nil {
							if err == badger.ErrKeyNotFound {
								val = nil
							} else {
								log.Panic(err)
							}
						}
						resp.Responses = append(resp.Responses, &raft_cmdpb.Response{
							CmdType: raft_cmdpb.CmdType_Get,
							Get: &raft_cmdpb.GetResponse{
								Value: val,
							},
						})
					case raft_cmdpb.CmdType_Put:
						txn.Set(engine_util.KeyWithCF(req.Put.Cf, req.Put.Key), req.Put.Value)
						log.Infof("%s put key(%s), val(%s)", d.Tag, req.Put.Key, req.Put.Value)
						resp.Responses = append(resp.Responses, &raft_cmdpb.Response{
							CmdType: raft_cmdpb.CmdType_Put,
							Put:     &raft_cmdpb.PutResponse{},
						})
					case raft_cmdpb.CmdType_Delete:
						txn.Delete(engine_util.KeyWithCF(req.Delete.Cf, req.Delete.Key))
						resp.Responses = append(resp.Responses, &raft_cmdpb.Response{
							CmdType: raft_cmdpb.CmdType_Delete,
							Delete:  &raft_cmdpb.DeleteResponse{},
						})
					case raft_cmdpb.CmdType_Snap:
						resp.Responses = append(resp.Responses, &raft_cmdpb.Response{
							CmdType: raft_cmdpb.CmdType_Snap,
							Snap: &raft_cmdpb.SnapResponse{
								Region: cloneRegion(d.Region()),
							},
						})
					}
				} else {
					d.addEmptyResponse(resp, req)
				}
			}
			d.peerStorage.applyState.AppliedIndex = entry.Index
			val, err := d.peerStorage.applyState.Marshal()
			if err != nil {
				log.Panic(err)
			}
			txn.Set(meta.ApplyStateKey(d.regionId), val)
			err = txn.Commit()
			if err != nil {
				log.Panic(err)
			}

			if d.IsLeader() {
				d.handleProposals(entry, resp)
			}
		}

		//admin request
		if msg.AdminRequest != nil {
			kvWB := new(engine_util.WriteBatch)
			switch msg.AdminRequest.CmdType {
			case raft_cmdpb.AdminCmdType_CompactLog:
				if msg.AdminRequest.CompactLog.CompactIndex > d.peerStorage.applyState.TruncatedState.Index { //可能因为本peer因为接受snapshot，导致没有这段日志，此时不应用此gc log
					d.peerStorage.applyState.TruncatedState.Index = msg.AdminRequest.CompactLog.CompactIndex
					d.peerStorage.applyState.TruncatedState.Term = msg.AdminRequest.CompactLog.CompactTerm
					d.ScheduleCompactLog(msg.AdminRequest.CompactLog.CompactIndex)
				}
			case raft_cmdpb.AdminCmdType_Split:
				var duplicated, stale bool
				splitReq := msg.AdminRequest.Split
				region := d.Region()

				if util.IsEpochStale(msg.Header.GetRegionEpoch(), region.RegionEpoch) {
					log.Infof("%s split request region epoch not match, now(%v), req(%v)", d.Tag, region.RegionEpoch, msg.Header.GetRegionEpoch())
					stale = true
				}

				if !regionContainsKey(region, splitReq.SplitKey) {
					duplicated = true
				}

				if !duplicated && !stale {
					log.Infof("%s split on key(%x), new peers(%v)", d.Tag, splitReq.SplitKey, splitReq.NewPeerIds)
					region.RegionEpoch.Version += 1

					newRegion := new(metapb.Region)
					d.initNewRegion(newRegion, region, splitReq)
					newPeer, err := createPeer(d.Meta.StoreId, d.ctx.cfg, d.ctx.regionTaskSender, d.ctx.engine, newRegion)
					if err != nil {
						log.Panicf("%s create peer failed on split, err(%v)", d.Tag, err)
					}

					region.EndKey = splitReq.SplitKey

					d.ctx.storeMeta.Lock()
					d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: region})
					d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: newRegion})
					d.ctx.storeMeta.setRegion(newRegion, newPeer)
					d.ctx.storeMeta.Unlock()

					d.ctx.router.register(newPeer)
					err = d.ctx.router.send(newRegion.Id, message.Msg{RegionID: newRegion.Id, Type: message.MsgTypeStart})
					if err != nil {
						log.Panicf("%s send start msg to new region failed", d.Tag)
					}
					meta.WriteRegionState(kvWB, region, rspb.PeerState_Normal)
					meta.WriteRegionState(kvWB, newRegion, rspb.PeerState_Normal)
					if d.IsLeader() {
						d.notifyHeartbeatScheduler(region, d.peer)
						d.notifyHeartbeatScheduler(newRegion, newPeer)
					}
				} else {
					log.Infof("%s skipped a duplicated or stale split request", d.Tag)
				}
			}
			d.peerStorage.applyState.AppliedIndex = entry.Index
			kvWB.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
			kvWB.MustWriteToDB(d.peerStorage.Engines.Kv)
		}
	} else if entry.EntryType == eraftpb.EntryType_EntryConfChange {
		var cc eraftpb.ConfChange
		if err := cc.Unmarshal(entry.Data); err != nil {
			log.Panic(err)
		}
		kvWB := new(engine_util.WriteBatch)
		region := d.Region()
		var skip bool
		if cc.ChangeType == eraftpb.ConfChangeType_AddNode {
			for _, peer := range region.Peers {
				if peer.Id == cc.NodeId { //已经有了
					skip = true
					break
				}
			}
			if !skip {
				region.RegionEpoch.ConfVer += 1
				storeId := binary.BigEndian.Uint64(cc.Context)
				region.Peers = append(region.Peers, &metapb.Peer{Id: cc.NodeId, StoreId: storeId})
				log.Infof("%v region(%d) add node (%d), peers(%v)", d.Tag, d.regionId, cc.NodeId, region.Peers)
			}
		} else if cc.ChangeType == eraftpb.ConfChangeType_RemoveNode {
			skip = true
			var idx int
			for i, peer := range region.Peers {
				if peer.Id == cc.NodeId {
					skip = false
					idx = i
					break
				}
			}
			if !skip {
				region.RegionEpoch.ConfVer += 1
				region.Peers = append(region.Peers[:idx], region.Peers[idx+1:]...)
				d.removePeerCache(cc.NodeId)
				log.Infof("%v region(%d) remove node (%d), peers(%v)", d.Tag, d.regionId, cc.NodeId, region.Peers)
			}
		} else {
			log.Panic("invalid ConfChange Type %v", cc.ChangeType)
		}

		if !skip {
			meta.WriteRegionState(kvWB, region, rspb.PeerState_Normal)
			d.peerStorage.applyState.AppliedIndex = entry.Index
			kvWB.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
			kvWB.MustWriteToDB(d.ctx.engine.Kv)

			d.RaftGroup.ApplyConfChange(cc)

			if d.IsLeader() {
				d.notifyHeartbeatScheduler(region, d.peer)
			}
			if cc.ChangeType == eraftpb.ConfChangeType_RemoveNode && cc.NodeId == d.PeerId() {
				log.Infof("%s destroy at index(%d)", d.Tag, entry.Index)
				d.destroyPeer()
			}
		} else {
			log.Infof("%s skip a conf change, index(%d)", d.Tag, entry.Index)
			d.peerStorage.applyState.AppliedIndex = entry.Index
			kvWB.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
			kvWB.MustWriteToDB(d.ctx.engine.Kv)
		}
	}
}

func (d *peerMsgHandler) addEmptyResponse(resp *raft_cmdpb.RaftCmdResponse, req *raft_cmdpb.Request) {
	switch req.CmdType {
	case raft_cmdpb.CmdType_Get:
		resp.Responses = append(resp.Responses, &raft_cmdpb.Response{
			CmdType: raft_cmdpb.CmdType_Get,
			Get:     &raft_cmdpb.GetResponse{},
		})
	case raft_cmdpb.CmdType_Put:
		resp.Responses = append(resp.Responses, &raft_cmdpb.Response{
			CmdType: raft_cmdpb.CmdType_Put,
			Put:     &raft_cmdpb.PutResponse{},
		})
	case raft_cmdpb.CmdType_Delete:
		resp.Responses = append(resp.Responses, &raft_cmdpb.Response{
			CmdType: raft_cmdpb.CmdType_Delete,
			Delete:  &raft_cmdpb.DeleteResponse{},
		})
	case raft_cmdpb.CmdType_Snap:
		resp.Responses = append(resp.Responses, &raft_cmdpb.Response{
			CmdType: raft_cmdpb.CmdType_Snap,
			Snap:    &raft_cmdpb.SnapResponse{},
		})
	default:
		log.Panicf("invalid request cmd type")
	}
}

func (d *peerMsgHandler) handleProposals(entry *eraftpb.Entry, resp *raft_cmdpb.RaftCmdResponse) {
	for len(d.proposals) > 0 {
		proposal := d.proposals[0]
		if entry.Term < proposal.term {
			return
		}
		if entry.Term > proposal.term {
			proposal.cb.Done(ErrRespStaleCommand(proposal.term))
			d.proposals = d.proposals[1:]
			continue
		}

		if entry.Index < proposal.index {
			return
		}

		if entry.Index > proposal.index {
			proposal.cb.Done(ErrRespStaleCommand(proposal.term))
			d.proposals = d.proposals[1:]
			continue
		}

		if entry.Index == proposal.index {
			if proposal.cb != nil {
				proposal.cb.Txn = d.peerStorage.Engines.Kv.NewTransaction(false)
			}
			proposal.cb.Done(resp)
			d.proposals = d.proposals[1:]
			return
		}

		log.Panicf("cant reach here, dead loop?")
	}
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
		log.Infof("%s finished to handle message.MsgTypeGcSnap", d.Tag)
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
		log.Infof("%v return NotLeader, Leader(%d)", d.Tag, leaderID)
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

func (d *peerMsgHandler) cannotRemove(cc eraftpb.ConfChange) bool {
	return cc.ChangeType == eraftpb.ConfChangeType_RemoveNode && d.IsLeader() && d.PeerId() == cc.NodeId && len(d.Region().Peers) == 2
}

func isKeyCmd(req *raft_cmdpb.Request) bool {
	switch req.CmdType {
	case raft_cmdpb.CmdType_Put, raft_cmdpb.CmdType_Get, raft_cmdpb.CmdType_Delete:
		return true
	default:
		return false
	}
}

func getKeyFromRequest(req *raft_cmdpb.Request) []byte {
	switch req.CmdType {
	case raft_cmdpb.CmdType_Put:
		return req.Put.Key
	case raft_cmdpb.CmdType_Get:
		return req.Get.Key
	case raft_cmdpb.CmdType_Delete:
		return req.Delete.Key
	}
	return []byte{0}
}

func regionContainsKey(region *metapb.Region, key []byte) bool {
	start, end := region.GetStartKey(), region.GetEndKey()
	return bytes.Compare(key, start) >= 0 && (len(end) == 0 || bytes.Compare(key, end) < 0)
}

func (d *peerMsgHandler) proposeRaftCommand(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	err := d.preProposeRaftCommand(msg)
	if err != nil {
		cb.Done(ErrResp(err))
		return
	}
	// Your Code Here (2B).

	d.proposals = append(d.proposals, &proposal{
		index: d.nextProposalIndex(),
		term:  d.Term(),
		cb:    cb,
	})

	data, err := msg.Marshal()
	if err != nil {
		log.Panic(err)
	}
	if msg.AdminRequest != nil {
		if msg.AdminRequest.TransferLeader != nil { // TransferLeader的request不需要复制
			d.RaftGroup.TransferLeader(msg.AdminRequest.TransferLeader.Peer.Id)
			resp := newCmdResp()
			resp.AdminResponse = &raft_cmdpb.AdminResponse{
				CmdType:        raft_cmdpb.AdminCmdType_TransferLeader,
				TransferLeader: &raft_cmdpb.TransferLeaderResponse{},
			}
			cb.Done(resp)
			return
		}

		if msg.AdminRequest.ChangePeer != nil {
			cp := msg.AdminRequest.ChangePeer
			cc := eraftpb.ConfChange{ChangeType: cp.ChangeType, NodeId: cp.Peer.Id}

			if d.cannotRemove(cc) {
				var target uint64
				for _, peer := range d.Region().Peers {
					if peer.Id != d.PeerId() {
						target = peer.Id
					}
				}
				d.RaftGroup.TransferLeader(target)
				log.Infof("%v can not remove self, transferLeader to %x", d.Tag, target)
				return
			}

			cc.Context = make([]byte, 8)
			binary.BigEndian.PutUint64(cc.Context, cp.Peer.StoreId)
			d.RaftGroup.ProposeConfChange(cc)
			return
		}

		if msg.AdminRequest.Split != nil {
			splitReq := msg.AdminRequest.Split
			region := d.Region()
			if !regionContainsKey(region, splitReq.SplitKey) {
				cb.Done(ErrRespStaleCommand(d.Term()))
				log.Infof("%s skip a split request, region(%v), split key(%v)", d.Tag, region, splitReq.SplitKey)
				return
			}
		}
	}
	d.RaftGroup.Propose(data)
}

func (d *peerMsgHandler) notifyHeartbeatScheduler(region *metapb.Region, p *peer) {
	clonedRegion := new(metapb.Region)
	err := util.CloneMsg(region, clonedRegion)
	if err != nil {
		return
	}
	d.ctx.schedulerTaskSender <- &runner.SchedulerRegionHeartbeatTask{
		Region:          clonedRegion,
		Peer:            p.Meta,
		PendingPeers:    p.CollectPendingPeers(),
		ApproximateSize: p.ApproximateSize,
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
	log.Debugf("%s handle raft message %s from %d to %d",
		d.Tag, msg.GetMessage().GetMsgType(), msg.GetFromPeer().GetId(), msg.GetToPeer().GetId())
	if !d.validateRaftMessage(msg) {
		return nil
	}
	if d.stopped {
		log.Infof("%v onRaftMsg peer already stopped, ignore msg\n", d.Tag)
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
	if to.GetStoreId() != d.storeID() { // 检查store是否匹配
		log.Warnf("[region %d] store not match, to store id %d, mine %d, ignore it",
			regionID, to.GetStoreId(), d.storeID())
		return false
	}
	if msg.RegionEpoch == nil { // 检查RegionEpoch
		log.Errorf("[region %d] missing epoch in raft message, ignore it", regionID)
		return false
	}
	return true
}

// / Checks if the message is sent to the correct peer.
// /
// / Returns true means that the message can be dropped silently.
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
	msgType := msg.Message.GetMsgType()

	if !needGC {
		log.Infof("[region %d] raft message %s is stale, current %v ignore it",
			regionID, msgType, curEpoch)
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
	log.Infof("%s snapshot[index %d term %d]region info meta.regions: %s \n d.Region(): %s", d.Tag, snapshot.Metadata.Index, snapshot.Metadata.Term, meta.regions[d.regionId], d.Region())
	if !util.RegionEqual(meta.regions[d.regionId], d.Region()) { //什么情况下会不一致呢？
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

func (d *peerMsgHandler) destroyPeer() { //自我销毁
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
	if meta.regionRanges.Delete(&regionItem{region: d.Region()}) == nil && isInitialized { //error_timeout2.2，这里isInitialized为false，导致regionRanges里面东西没删？ 调换顺序
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
	d.proposeRaftCommand(request, nil) // 只有leader会主动截断日志
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

	if !d.IsLeader() { //只有leader才向Scheduler上报消息
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
				log.Infof("%s in store %d snap file %s has been compacted, delete", d.Tag, d.ctx.store.Id, key)
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
