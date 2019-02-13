package fnConsensus

import (
	"sync"

	"github.com/tendermint/tendermint/p2p"
	"github.com/tendermint/tendermint/p2p/conn"

	dbm "github.com/tendermint/tendermint/libs/db"
)

const FnVoteSetChannelID = byte(0x50)
const FnStateChannelID = byte(0x51)

type FnConsensusReactor struct {
	p2p.BaseReactor

	connectedPeers map[p2p.ID]p2p.Peer
	mtx            sync.RWMutex
	state          *ReactorState
	db             dbm.DB
}

func NewFnConsensusReactor(db dbm.DB) *FnConsensusReactor {
	reactor := &FnConsensusReactor{
		connectedPeers: make(map[p2p.ID]p2p.Peer),
		db:             db,
	}

	reactor.BaseReactor = *p2p.NewBaseReactor("FnConsensusReactor", reactor)
	return reactor
}

func (f *FnConsensusReactor) OnStart() error {
	reactorState, err := LoadReactorState(f.db)
	if err != nil {
		return err
	}
	f.state = reactorState
	return nil
}

// GetChannels returns the list of channel descriptors.
func (f *FnConsensusReactor) GetChannels() []*conn.ChannelDescriptor {
	// Priorities are deliberately set to low, to prevent interfering with core TM
	return []*conn.ChannelDescriptor{
		{
			ID:                  FnStateChannelID,
			Priority:            25,
			SendQueueCapacity:   10,
			RecvBufferCapacity:  10,
			RecvMessageCapacity: 10,
		},
		{
			ID:                  FnVoteSetChannelID,
			Priority:            30,
			SendQueueCapacity:   10,
			RecvBufferCapacity:  10,
			RecvMessageCapacity: 10,
		},
	}
}

// AddPeer is called by the switch when a new peer is added.
func (f *FnConsensusReactor) AddPeer(peer p2p.Peer) {
	f.mtx.Lock()
	defer f.mtx.Unlock()
	f.connectedPeers[peer.ID()] = peer
	// Start go routine for state sync
	// Start go routine for vote sync
}

// RemovePeer is called by the switch when the peer is stopped (due to error
// or other reason).
func (f *FnConsensusReactor) RemovePeer(peer p2p.Peer, reason interface{}) {
	f.mtx.Lock()
	defer f.mtx.Unlock()
	// Stop go routine for state sync
	// Stop go routine for vote sync
	delete(f.connectedPeers, peer.ID())
}

// Receive is called when msgBytes is received from peer.
//
// NOTE reactor can not keep msgBytes around after Receive completes without
// copying.
//
// CONTRACT: msgBytes are not nil.
func (f *FnConsensusReactor) Receive(chID byte, peer p2p.Peer, msgBytes []byte) {

}
