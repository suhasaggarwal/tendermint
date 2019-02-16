package fnConsensus

import (
	"fmt"
	"sync"
	"time"

	"github.com/tendermint/tendermint/p2p"
	"github.com/tendermint/tendermint/state"
	"github.com/tendermint/tendermint/types"

	dbm "github.com/tendermint/tendermint/libs/db"
)

const FnVoteSetChannel = byte(0x50)

const maxMsgSize = 200 * 1024

type FnConsensusReactor struct {
	p2p.BaseReactor

	connectedPeers map[p2p.ID]p2p.Peer
	state          *ReactorState
	db             dbm.DB
	tmStateDB      dbm.DB
	chainID        string

	fnRegistry FnRegistry

	fnProposer FnProposer

	privValidator types.PrivValidator

	peerMapMtx sync.RWMutex
}

func NewFnConsensusReactor(chainID string, privValidator types.PrivValidator, fnProposer FnProposer, fnRegistry FnRegistry, db dbm.DB, tmStateDB dbm.DB) *FnConsensusReactor {
	reactor := &FnConsensusReactor{
		connectedPeers: make(map[p2p.ID]p2p.Peer),
		db:             db,
		chainID:        chainID,
		tmStateDB:      tmStateDB,
		fnRegistry:     fnRegistry,
		privValidator:  privValidator,
		fnProposer:     fnProposer,
	}

	reactor.BaseReactor = *p2p.NewBaseReactor("FnConsensusReactor", reactor)
	return reactor
}

func (f *FnConsensusReactor) String() string {
	return "FnConsensusReactor"
}

func (f *FnConsensusReactor) OnStart() error {
	reactorState, err := LoadReactorState(f.db)
	if err != nil {
		return err
	}
	f.state = reactorState
	go f.proposalReceiverRoutine()
	return nil
}

// GetChannels returns the list of channel descriptors.
func (f *FnConsensusReactor) GetChannels() []*p2p.ChannelDescriptor {
	// Priorities are deliberately set to low, to prevent interfering with core TM
	return []*p2p.ChannelDescriptor{
		{
			ID:                  FnVoteSetChannel,
			Priority:            25,
			SendQueueCapacity:   100,
			RecvMessageCapacity: maxMsgSize,
		},
	}
}

// AddPeer is called by the switch when a new peer is added.
func (f *FnConsensusReactor) AddPeer(peer p2p.Peer) {
	f.peerMapMtx.Lock()
	defer f.peerMapMtx.Unlock()
	f.connectedPeers[peer.ID()] = peer
}

// RemovePeer is called by the switch when the peer is stopped (due to error
// or other reason).
func (f *FnConsensusReactor) RemovePeer(peer p2p.Peer, reason interface{}) {
	f.peerMapMtx.Lock()
	defer f.peerMapMtx.Unlock()
	delete(f.connectedPeers, peer.ID())
}

func (f *FnConsensusReactor) areWeValidator(currentValidatorSet *types.ValidatorSet) (bool, int) {
	validatorIndex, _ := currentValidatorSet.GetByAddress(f.privValidator.GetPubKey().Address())
	return validatorIndex != -1, validatorIndex
}

func (f *FnConsensusReactor) proposalReceiverRoutine() {
OUTER_LOOP:
	for {
		select {
		case fnID := <-f.fnProposer.ProposeChannel():
			currentState := state.LoadState(f.tmStateDB)

			areWeValidator, validatorIndex := f.areWeValidator(currentState.Validators)
			if !areWeValidator {
				f.Logger.Error("FnConsensusReactor: unable to propose new Fn, as we are no longer validator")
				return
			}

			executionRequest, err := NewFnExecutionRequest(fnID, f.fnRegistry)
			if err != nil {
				f.Logger.Error("FnConsensusReactor: unable to create Fn execution request as FnID is invalid", "fnID", fnID)
				break
			}

			fn := f.fnRegistry.Get(fnID)

			hash, signature, err := fn.GetMessageAndSignature()
			if err != nil {
				f.Logger.Error("FnConsensusReactor: received error while executing fn.GetMessageAndSignature", "fnID", fnID)
				break
			}

			nonce, err := fn.GetNonce()
			if err != nil {
				f.Logger.Error("FnConsensusReactor: unable to get nonce from fn.GetNonce method", "fnID", fnID)
				break
			}

			if lastSeenNonce, ok := f.state.LastSeenNonces[fnID]; ok {
				if nonce <= lastSeenNonce {
					f.Logger.Error("FnConsensusError: nonce is already seen")
					break
				}
			}

			individualExecution := &FnIndividualExecutionResponse{
				Error:           "",
				Hash:            hash,
				OracleSignature: signature,
				Status:          0,
			}

			executionResponse := NewFnExecutionResponse(individualExecution, validatorIndex, currentState.Validators)

			votesetPayload := NewFnVotePayload(executionRequest, executionResponse)

			voteSet, err := NewVoteSet(f.chainID, nonce, 1*time.Minute, validatorIndex, votesetPayload, f.privValidator, currentState.Validators)
			if err != nil {
				f.Logger.Error("FnConsensusReactor: unable to create new voteset", "fnID", fnID, "error", err)
				break
			}

			// It seems we are the only validator, so return the signature and close the case.
			if voteSet.IsMaj23(currentState.Validators) {
				fn.SubmitMultiSignedMessage(voteSet.Payload.Response.Hash, voteSet.Payload.Response.OracleSignatures)
				break
			}

			if f.state.CurrentVoteSets[fnID] != nil {
				f.Logger.Error("[Warn] FnConsensusReactor: we are overwriting another voteset", "fnID", fnID)
			}

			f.state.CurrentVoteSets[fnID] = voteSet
			f.state.LastSeenNonces[fnID] = nonce

			if err := SaveReactorState(f.db, f.state, true); err != nil {
				f.Logger.Error("FnConsensusReactor: unable to save state", "fnID", fnID, "error", err)
				break
			}

			marshalledBytes, err := voteSet.Marshal()
			if err != nil {
				f.Logger.Error(fmt.Sprintf("FnConsensusReactor: Unable to marshal currentVoteSet at FnID: %s", fnID))
				break
			}

			f.peerMapMtx.RLock()
			for _, peer := range f.connectedPeers {
				go func() {
					// TODO: Handle timeout
					peer.Send(FnVoteSetChannel, marshalledBytes)
				}()
			}
			f.peerMapMtx.RUnlock()

			break
		case <-f.Quit():
			f.Logger.Info("received signal from quitCh, Quitting...")
			break OUTER_LOOP
		}
	}
}

// Receive is called when msgBytes is received from peer.
//
// NOTE reactor can not keep msgBytes around after Receive completes without
// copying.
//
// CONTRACT: msgBytes are not nil.
func (f *FnConsensusReactor) Receive(chID byte, sender p2p.Peer, msgBytes []byte) {
	currentState := state.LoadState(f.tmStateDB)
	areWeValidator, validatorIndex := f.areWeValidator(currentState.Validators)
	var err error

	switch chID {
	case FnVoteSetChannel:
		remoteVoteSet := &FnVoteSet{}
		if err := remoteVoteSet.Unmarshal(msgBytes); err != nil {
			f.Logger.Error("FnConsensusReactor: Invalid Data passed, ignoring...")
			return
		}

		if lastSeenNonce, ok := f.state.LastSeenNonces[remoteVoteSet.GetFnID()]; ok {
			if remoteVoteSet.Nonce < lastSeenNonce {
				f.Logger.Error("FnConsensusError: nonce is already processed")
				return
			}
		}

		if !remoteVoteSet.IsValid(f.chainID, currentState.Validators, f.fnRegistry) {
			f.Logger.Error("FnConsensusReactor: Invalid VoteSet specified, ignoring...")
			return
		}

		if remoteVoteSet.IsMaj23(currentState.Validators) {
			f.Logger.Error("FnConsensusReactor: Protocol violation: Received VoteSet with majority of validators signed, Ignoring...")
			return
		}

		didWeContribute := false
		fnID := remoteVoteSet.GetFnID()
		fn := f.fnRegistry.Get(fnID)

		// TODO: Check nonce with mainnet before accepting remote vote set

		if f.state.CurrentVoteSets[fnID] == nil {
			f.state.CurrentVoteSets[fnID] = remoteVoteSet
			didWeContribute = false
		} else {
			if didWeContribute, err = f.state.CurrentVoteSets[fnID].Merge(remoteVoteSet); err != nil {
				f.Logger.Error("FnConsensusReactor: Unable to merge remote vote set into our own.", "error:", err)
				return
			}
		}

		if areWeValidator {
			hash, signature, err := fn.GetMessageAndSignature()
			if err != nil {
				f.Logger.Error("FnConsensusReactor: fn.GetMessageAndSignature returned an error, ignoring..")
				return
			}

			err = f.state.CurrentVoteSets[fnID].AddVote(&FnIndividualExecutionResponse{
				Status:          0,
				Error:           "",
				Hash:            hash,
				OracleSignature: signature,
			}, currentState.Validators, validatorIndex, f.privValidator)
			if err != nil {
				f.Logger.Error("FnConsensusError: unable to add vote to current voteset, ignoring...")
				return
			}

			didWeContribute = true
		}

		f.state.LastSeenNonces[fnID] = remoteVoteSet.Nonce
		if err := SaveReactorState(f.db, f.state, true); err != nil {
			f.Logger.Error("FnConsensusReactor: unable to save state", "fnID", fnID, "error", err)
			return
		}

		if areWeValidator {
			if f.state.CurrentVoteSets[fnID].IsMaj23(currentState.Validators) {
				fn.SubmitMultiSignedMessage(f.state.CurrentVoteSets[fnID].Payload.Response.Hash, f.state.CurrentVoteSets[fnID].Payload.Response.OracleSignatures)
				return
			}
		}

		marshalledBytes, err := f.state.CurrentVoteSets[fnID].Marshal()
		if err != nil {
			f.Logger.Error(fmt.Sprintf("FnConsensusReactor: Unable to marshal currentVoteSet at FnID: %s", fnID))
			return
		}

		f.peerMapMtx.RLock()
		for peerID, peer := range f.connectedPeers {

			// If we didnt contribute to remote vote, no need to pass it to sender
			if !didWeContribute {
				if peerID == sender.ID() {
					continue
				}
			}

			go func() {
				// TODO: Handle timeout
				peer.Send(FnVoteSetChannel, marshalledBytes)
			}()
		}
		f.peerMapMtx.RUnlock()

		break
	default:
		f.Logger.Error("FnConsensusReactor: Unknown channel: %v", chID)
	}
}
