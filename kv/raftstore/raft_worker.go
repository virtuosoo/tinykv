package raftstore

import (
	"sync"

	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/log"
)

// raftWorker is responsible for run raft commands and apply raft logs.
type raftWorker struct {
	pr *router

	// receiver of messages should sent to raft, including:
	// * raft command from `raftStorage`
	// * raft inner messages from other peers sent by network
	raftCh chan message.Msg
	ctx    *GlobalContext

	closeCh <-chan struct{}
}

func newRaftWorker(ctx *GlobalContext, pm *router) *raftWorker {
	return &raftWorker{
		raftCh: pm.peerSender,
		ctx:    ctx,
		pr:     pm,
	}
}

// run runs raft commands.
// On each loop, raft commands are batched by channel buffer.
// After commands are handled, we collect apply messages by peers, make a applyBatch, send it to apply channel.
func (rw *raftWorker) run(closeCh <-chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()
	var msgs []message.Msg
	for {
		msgs = msgs[:0]
		select {
		case <-closeCh:
			log.Infof("%d raft worker received closeCh, return", rw.ctx.store.Id)
			return
		case msg := <-rw.raftCh:
			msgs = append(msgs, msg)
		}
		pending := len(rw.raftCh)
		for i := 0; i < pending; i++ {
			msgs = append(msgs, <-rw.raftCh)
		}
		peerStateMap := make(map[uint64]*peerState)
		for _, msg := range msgs {
			peerState := rw.getPeerState(peerStateMap, msg.RegionID)
			if peerState == nil {
				continue //没有这个peer，有可能消息在raft worker的receiver中堵着了，会销毁peer的消息也在队列中。
			}
			newPeerMsgHandler(peerState.peer, rw.ctx).HandleMsg(msg)
		}
		for _, peerState := range peerStateMap {
			newPeerMsgHandler(peerState.peer, rw.ctx).HandleRaftReady()
		}
	}
}

func (rw *raftWorker) getPeerState(peersMap map[uint64]*peerState, regionID uint64) *peerState {
	peer, ok := peersMap[regionID]
	if !ok {
		peer = rw.pr.get(regionID)
		if peer == nil {
			return nil
		}
		peersMap[regionID] = peer
	}
	return peer
}
