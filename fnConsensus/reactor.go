package fnConsensus

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/tendermint/tendermint/p2p"
	"github.com/tendermint/tendermint/state"
	"github.com/tendermint/tendermint/types"

	dbm "github.com/tendermint/tendermint/libs/db"

	"crypto/sha512"
)

const FnVoteSetChannel = byte(0x50)

const maxMsgSize = 500 * 1024
const ProposeDuration = 30 * time.Second

type FnConsensusReactor struct {
	p2p.BaseReactor

	connectedPeers map[p2p.ID]p2p.Peer
	state          *ReactorState
	db             dbm.DB
	tmStateDB      dbm.DB
	chainID        string

	fnRegistry FnRegistry

	privValidator types.PrivValidator

	peerMapMtx sync.RWMutex

	stateMtx sync.Mutex
}

func NewFnConsensusReactor(chainID string, privValidator types.PrivValidator, fnRegistry FnRegistry, db dbm.DB, tmStateDB dbm.DB) *FnConsensusReactor {
	reactor := &FnConsensusReactor{
		connectedPeers: make(map[p2p.ID]p2p.Peer),
		db:             db,
		chainID:        chainID,
		tmStateDB:      tmStateDB,
		fnRegistry:     fnRegistry,
		privValidator:  privValidator,
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
	go f.proposalRoutine()
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

func (f *FnConsensusReactor) calculateMessageHash(message []byte) ([]byte, error) {
	hash := sha512.New()
	_, err := hash.Write(message)
	if err != nil {
		return nil, err
	}
	return hash.Sum(nil), nil
}

func (f *FnConsensusReactor) proposalRoutine() {
	ticker := time.NewTicker(ProposeDuration)

OUTER_LOOP:
	for {
		select {
		case <-ticker.C:
			currentState := state.LoadState(f.tmStateDB)

			areWeValidator, ownValidatorIndex := f.areWeValidator(currentState.Validators)
			if !areWeValidator {
				f.Logger.Error("FnConsensusReactor: unable to propose new Fn, as we are no longer validator")
				break
			}

			proposer := currentState.Validators.GetProposer()
			if proposer == nil {
				f.Logger.Error("FnConsensusReactor: unable to get proposer from current validators")
				break
			}

			proposerIndex, _ := currentState.Validators.GetByAddress(proposer.Address)

			if proposerIndex != ownValidatorIndex {
				break
			}

			fnIDs := f.fnRegistry.GetAll()
			sort.Strings(fnIDs)

			for _, fnID := range fnIDs {
				fn := f.fnRegistry.Get(fnID)
				nonce, err := fn.GetNonce()
				if err != nil {
					f.Logger.Error("FnConsensusReactor: error while trying to get nonce for fn", "FnID", fnID, "error", err)
					continue
				}

				f.propose(fnID, fn, nonce, currentState, ownValidatorIndex)
			}
			break
		case <-f.Quit():
			f.Logger.Info("FnConsensusReactor: shutting down the loop because we are shutting down")
			ticker.Stop()
			break OUTER_LOOP
		}
	}
}

func (f *FnConsensusReactor) propose(fnID string, fn Fn, nonce int64, currentState state.State, validatorIndex int) {
	message, signature, err := fn.GetMessageAndSignature()
	if err != nil {
		f.Logger.Error("FnConsensusReactor: received error while executing fn.GetMessageAndSignature", "fnID", fnID)
		return
	}

	hash, err := f.calculateMessageHash(message)
	if err != nil {
		f.Logger.Error("FnConsensusReactor: unable to calculate message hash", "fnID", fnID, "error", err)
		return
	}

	if err = fn.MapMessage(hash, message); err != nil {
		f.Logger.Error("FnConsensusReactor: received error while executing fn.MapMessage", "fnID", fnID, "error", err)
		return
	}

	executionRequest, err := NewFnExecutionRequest(fnID, f.fnRegistry)
	if err != nil {
		f.Logger.Error("FnConsensusReactor: unable to create Fn execution request as FnID is invalid", "fnID", fnID)
		return
	}

	executionResponse := NewFnExecutionResponse(&FnIndividualExecutionResponse{
		Error:           "",
		Hash:            hash,
		OracleSignature: signature,
		Status:          0,
	}, validatorIndex, currentState.Validators)

	votesetPayload := NewFnVotePayload(executionRequest, executionResponse)

	voteSet, err := NewVoteSet(f.chainID, nonce, 1*time.Minute, validatorIndex, votesetPayload, f.privValidator, currentState.Validators)
	if err != nil {
		f.Logger.Error("FnConsensusReactor: unable to create new voteset", "fnID", fnID, "error", err)
		return
	}

	f.stateMtx.Lock()

	if lastSeenNonce, ok := f.state.LastSeenNonces[fnID]; ok {
		if nonce < lastSeenNonce {
			f.Logger.Error("FnConsensusError: nonce is already seen")
			f.stateMtx.Unlock()
			return
		} else if nonce == lastSeenNonce {
			f.Logger.Info("FnConsensusReactor: Nonce stayed the same, looks like previous round hasnt completed, skipping propose...")
			f.stateMtx.Unlock()
			return
		}
	}

	if f.state.CurrentVoteSets[fnID] != nil {
		f.Logger.Error("[Warn] FnConsensusReactor: we are overwriting previous voteset", "fnID", fnID)
	}

	// It seems we are the only validator, so return the signature and close the case.
	if voteSet.IsMaj23(currentState.Validators) {
		fn.SubmitMultiSignedMessage(voteSet.Payload.Response.Hash, voteSet.Payload.Response.OracleSignatures)
		f.stateMtx.Unlock()
		return
	}

	f.state.CurrentVoteSets[fnID] = voteSet
	f.state.LastSeenNonces[fnID] = nonce

	if err := SaveReactorState(f.db, f.state, true); err != nil {
		f.Logger.Error("FnConsensusReactor: unable to save state", "fnID", fnID, "error", err)
		f.stateMtx.Unlock()
		return
	}

	f.stateMtx.Unlock()

	marshalledBytes, err := voteSet.Marshal()
	if err != nil {
		f.Logger.Error(fmt.Sprintf("FnConsensusReactor: Unable to marshal currentVoteSet at FnID: %s", fnID))
		return
	}

	f.peerMapMtx.RLock()
	for _, peer := range f.connectedPeers {
		go func() {
			// TODO: Handle timeout
			peer.Send(FnVoteSetChannel, marshalledBytes)
		}()
	}
	f.peerMapMtx.RUnlock()
}

func (f *FnConsensusReactor) handleVoteSetChannelMessage(sender p2p.Peer, msgBytes []byte) {
	currentState := state.LoadState(f.tmStateDB)
	areWeValidator, validatorIndex := f.areWeValidator(currentState.Validators)
	var err error

	f.stateMtx.Lock()
	defer f.stateMtx.Unlock()

	remoteVoteSet := &FnVoteSet{}
	if err := remoteVoteSet.Unmarshal(msgBytes); err != nil {
		f.Logger.Error("FnConsensusReactor: Invalid Data passed, ignoring...")
		return
	}

	if lastSeenNonce, ok := f.state.LastSeenNonces[remoteVoteSet.GetFnID()]; ok {
		if remoteVoteSet.Nonce < lastSeenNonce {
			f.Logger.Error("FnConsensusReactor: nonce is already processed")
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

	var didWeContribute, hasOurVoteSetChanged bool
	fnID := remoteVoteSet.GetFnID()
	fn := f.fnRegistry.Get(fnID)

	// TODO: Check nonce with mainnet before accepting remote vote set

	if f.state.CurrentVoteSets[fnID] == nil {
		f.state.CurrentVoteSets[fnID] = remoteVoteSet
		// We didnt contribute but, our voteset changed
		didWeContribute = false
		hasOurVoteSetChanged = true
	} else {
		if didWeContribute, err = f.state.CurrentVoteSets[fnID].Merge(remoteVoteSet); err != nil {
			f.Logger.Error("FnConsensusReactor: Unable to merge remote vote set into our own.", "error:", err)
			return
		}
		hasOurVoteSetChanged = didWeContribute
	}

	if areWeValidator {
		message, signature, err := fn.GetMessageAndSignature()
		if err != nil {
			f.Logger.Error("FnConsensusReactor: fn.GetMessageAndSignature returned an error, ignoring..")
			return
		}

		hash, err := f.calculateMessageHash(message)
		if err != nil {
			f.Logger.Error("FnConsensusReactor: unable to calculate message hash", "fnID", fnID, "error", err)
			return
		}

		if err = fn.MapMessage(hash, message); err != nil {
			f.Logger.Error("FnConsensusReactor: received error while executing fn.MapMessage", "fnID", fnID, "error", err)
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
		hasOurVoteSetChanged = true
	}

	// Update last seen nonces
	f.state.LastSeenNonces[fnID] = remoteVoteSet.Nonce

	if err := SaveReactorState(f.db, f.state, true); err != nil {
		f.Logger.Error("FnConsensusReactor: unable to save state", "fnID", fnID, "error", err)
		return
	}

	if areWeValidator {
		// If we achieved Majority no need to propgate voteset to other peers
		if f.state.CurrentVoteSets[fnID].IsMaj23(currentState.Validators) {
			fn.SubmitMultiSignedMessage(f.state.CurrentVoteSets[fnID].Payload.Response.Hash, f.state.CurrentVoteSets[fnID].Payload.Response.OracleSignatures)
			return
		}
	}

	// If our vote havent't changed, no need to annonce it, as
	// we have already annonunced it last time it changed
	// TODO: If it is not maj23 vote, we should keep circulating it on pre-defined interval
	if !hasOurVoteSetChanged {
		return
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
}

// Receive is called when msgBytes is received from peer.
//
// NOTE reactor can not keep msgBytes around after Receive completes without
// copying.
//
// CONTRACT: msgBytes are not nil.
func (f *FnConsensusReactor) Receive(chID byte, sender p2p.Peer, msgBytes []byte) {

	switch chID {
	case FnVoteSetChannel:
		f.handleVoteSetChannelMessage(sender, msgBytes)
		break
	default:
		f.Logger.Error("FnConsensusReactor: Unknown channel: %v", chID)
	}
}
