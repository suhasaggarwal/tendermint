package fnConsensus

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/tendermint/tendermint/crypto"
	cmn "github.com/tendermint/tendermint/libs/common"
	"github.com/tendermint/tendermint/types"
)

var ErrFnVoteInvalidValidatorAddress = errors.New("invalid validator address for FnVote")
var ErrFnVoteInvalidSignature = errors.New("invalid validator signature")
var ErrFnVoteNotPresent = errors.New("Fn vote is not present for validator")
var ErrFnVoteAlreadyCasted = errors.New("Fn vote is already casted")

var ErrFnVoteMergeDiffPayload = errors.New("merging is not allowed, as votes have different payload")

type RequestPeerReactorState struct {
}

func (r *RequestPeerReactorState) Marshal() ([]byte, error) {
	return cdc.MarshalBinaryLengthPrefixed(r)
}

func (r *RequestPeerReactorState) Unmarshal(bz []byte) error {
	return cdc.UnmarshalBinaryLengthPrefixed(bz, r)
}

type ReactorState struct {
	CurrentVoteSets map[string]*FnVoteSet
}

func (p *ReactorState) Marshal() ([]byte, error) {
	voteSetMarshallable := &voteSetMarshallable{
		VoteSets: make([]*FnVoteSet, len(p.CurrentVoteSets)),
	}

	i := 0
	for _, voteSet := range p.CurrentVoteSets {
		voteSetMarshallable.VoteSets[i] = voteSet
		i++
	}

	return cdc.MarshalBinaryLengthPrefixed(voteSetMarshallable)
}

func (p *ReactorState) Unmarshal(bz []byte) error {
	voteSetMarshallable := &voteSetMarshallable{}
	if err := cdc.UnmarshalBinaryLengthPrefixed(bz, voteSetMarshallable); err != nil {
		return err
	}

	p.CurrentVoteSets = make(map[string]*FnVoteSet)

	for _, voteSet := range voteSetMarshallable.VoteSets {
		p.CurrentVoteSets[voteSet.Payload.Request.FnID] = voteSet
	}

	return nil
}

func NewReactorState(nonce int64, payload *FnVotePayload, valSet *types.ValidatorSet) *ReactorState {
	return &ReactorState{
		CurrentVoteSets: make(map[string]*FnVoteSet),
	}
}

type voteSetMarshallable struct {
	VoteSets []*FnVoteSet
}

type FnExecutionRequest struct {
	FnID string
}

func (f *FnExecutionRequest) Marshal() ([]byte, error) {
	return cdc.MarshalBinaryLengthPrefixed(f)
}

func (f *FnExecutionRequest) Unmarshal(bz []byte) error {
	return cdc.UnmarshalBinaryLengthPrefixed(bz, f)
}

func (f *FnExecutionRequest) compareInternal(remoteRequest *FnExecutionRequest) bool {
	return f.FnID != remoteRequest.FnID
}

func (f *FnExecutionRequest) Compare(remoteRequest *FnExecutionRequest) bool {
	return f.compareInternal(remoteRequest)
}

func (f *FnExecutionRequest) CannonicalCompare(remoteRequest *FnExecutionRequest) bool {
	return f.compareInternal(remoteRequest)
}

type FnExecutionResponse struct {
	Status    int64
	Error     string
	Hash      []byte
	Signature []byte
}

func (f *FnExecutionResponse) Marshal() ([]byte, error) {
	return cdc.MarshalBinaryLengthPrefixed(f)
}

func (f *FnExecutionResponse) Unmarshal(bz []byte) error {
	return cdc.UnmarshalBinaryLengthPrefixed(bz, f)
}

func (f *FnExecutionResponse) CannonicalCompare(remoteResponse *FnExecutionResponse) bool {
	if f.Error != remoteResponse.Error {
		return false
	}

	if f.Status != remoteResponse.Status {
		return false
	}

	if bytes.Compare(f.Hash, remoteResponse.Hash) != 0 {
		return false
	}

	return true
}

func (f *FnExecutionResponse) Compare(remoteResponse *FnExecutionResponse) bool {
	if !f.CannonicalCompare(remoteResponse) {
		return false
	}

	if bytes.Compare(f.Signature, remoteResponse.Signature) != 0 {
		return false
	}

	return true
}

type FnVotePayload struct {
	Request  *FnExecutionRequest  `json:"fn_execution_request"`
	Response *FnExecutionResponse `json:"fn_execution_response"`
}

func (f *FnVotePayload) Marshal() ([]byte, error) {
	return cdc.MarshalBinaryLengthPrefixed(f)
}

func (f *FnVotePayload) Unmarshal(bz []byte) error {
	return cdc.UnmarshalBinaryLengthPrefixed(bz, f)
}

func (f *FnVotePayload) IsValid() bool {
	if f.Request == nil || f.Response == nil {
		return false
	}

	return true
}

func (f *FnVotePayload) CannonicalCompare(remotePayload *FnVotePayload) bool {
	if remotePayload == nil || remotePayload.Request == nil || remotePayload.Response == nil {
		return false
	}

	if !f.Request.CannonicalCompare(remotePayload.Request) {
		return false
	}

	if !f.Response.CannonicalCompare(remotePayload.Response) {
		return false
	}

	return true
}

func (f *FnVotePayload) Compare(remotePayload *FnVotePayload) bool {
	if remotePayload == nil || remotePayload.Request == nil || remotePayload.Response == nil {
		return false
	}

	if !f.Request.Compare(remotePayload.Request) {
		return false
	}

	if !f.Response.Compare(remotePayload.Response) {
		return false
	}

	return true
}

type FnVoteSet struct {
	ChainID             string         `json:"chain_id"`
	TotalVotingPower    int64          `json:"total_voting_power"`
	Nonce               int64          `json:"nonce"`
	CreationTime        int64          `json:"creation_time"`
	ExpiresIn           int64          `json:"expires_in"`
	VoteBitArray        *cmn.BitArray  `json:"vote_bitarray"`
	Payload             *FnVotePayload `json:"vote_payload"`
	ValidatorSignatures [][]byte       `json:"signature"`
	ValidatorAddresses  [][]byte       `json:"validator_address"`
}

func NewVoteSet(nonce int64, payload *FnVotePayload, valSet *types.ValidatorSet) *FnVoteSet {
	voteBitArray := cmn.NewBitArray(valSet.Size())
	signatures := make([][]byte, valSet.Size())
	validatorAddresses := make([][]byte, valSet.Size())

	return &FnVoteSet{
		Nonce:               0,
		VoteBitArray:        voteBitArray,
		ValidatorSignatures: signatures,
		ValidatorAddresses:  validatorAddresses,
		Payload:             payload,
	}
}

func (voteSet *FnVoteSet) Marshal() ([]byte, error) {
	return cdc.MarshalBinaryLengthPrefixed(voteSet)
}

func (voteSet *FnVoteSet) Unmarshal(bz []byte) error {
	return cdc.UnmarshalBinaryLengthPrefixed(bz, voteSet)
}

func (voteSet *FnVoteSet) CannonicalCompare(remoteVoteSet *FnVoteSet) bool {
	if remoteVoteSet.Payload == nil {
		return false
	}

	if voteSet.Nonce != remoteVoteSet.Nonce {
		return false
	}

	if !voteSet.Payload.CannonicalCompare(remoteVoteSet.Payload) {
		return false
	}

	if len(voteSet.ValidatorSignatures) != len(remoteVoteSet.ValidatorSignatures) {
		return false
	}

	// For misbehaving nodes
	if len(voteSet.ValidatorAddresses) != len(remoteVoteSet.ValidatorAddresses) {
		return false
	}

	return true
}

func (voteSet *FnVoteSet) SignBytes(chainID string, validatorAddress []byte) ([]byte, error) {
	payloadBytes, err := voteSet.Payload.Marshal()
	if err != nil {
		return nil, err
	}

	prefix := []byte(fmt.Sprintf("NC:%d|CT:%d|EI:%d|CD:%s|VA:%s|PL:", voteSet.Nonce, voteSet.CreationTime,
		voteSet.ExpiresIn, chainID, hex.EncodeToString(validatorAddress)))

	signBytes := make([]byte, len(prefix)+len(payloadBytes))
	copy(signBytes, prefix)
	copy(signBytes[len(prefix):], payloadBytes)

	return signBytes, nil
}

func (voteSet *FnVoteSet) verifyInternal(signature []byte, chainID string, validatorAddress []byte, pubKey crypto.PubKey) error {
	if !bytes.Equal(pubKey.Address(), validatorAddress) {
		return ErrFnVoteInvalidValidatorAddress
	}

	signBytes, err := voteSet.SignBytes(chainID, validatorAddress)
	if err != nil {
		return err
	}

	if !pubKey.VerifyBytes(signBytes, signature) {
		return ErrFnVoteInvalidSignature
	}
	return nil
}

func (voteSet *FnVoteSet) IsExpired() bool {
	creationTime := time.Unix(voteSet.CreationTime, 0)
	expiresIn := time.Duration(voteSet.ExpiresIn)
	expiryTime := creationTime.Add(expiresIn)

	return expiryTime.Before(time.Now().UTC())
}

func (voteSet *FnVoteSet) GetFnID() string {
	return voteSet.Payload.Request.FnID
}

func (voteSet *FnVoteSet) IsMaj23(currentValidatorSet *types.ValidatorSet) bool {
	return voteSet.TotalVotingPower >= currentValidatorSet.TotalVotingPower()*2/3+1
}

// Should be the first function to be invoked on vote set received from Peer
func (voteSet *FnVoteSet) IsValid(chainID string, currentValidatorSet *types.ValidatorSet, registry *FnRegistry) bool {
	numValidators := voteSet.VoteBitArray.Size()

	var calculatedVotingPower int64

	if voteSet.Payload == nil {
		return false
	}

	if !voteSet.Payload.IsValid() {
		return false
	}

	if registry.Get(voteSet.GetFnID()) == nil {
		return false
	}

	if voteSet.ChainID != chainID {
		return false
	}

	if voteSet.IsExpired() {
		return false
	}

	if numValidators != len(voteSet.ValidatorAddresses) {
		return false
	}

	if numValidators != len(voteSet.ValidatorSignatures) {
		return false
	}

	if numValidators != currentValidatorSet.Size() {
		return false
	}

	currentValidatorSet.Iterate(func(i int, val *types.Validator) bool {
		if !voteSet.VoteBitArray.GetIndex(i) {
			return true
		}
		if err := voteSet.VerifyValidatorSign(i, chainID, val.PubKey); err != nil {
			return false
		}
		calculatedVotingPower += val.VotingPower
		return true
	})

	// Voting power contained in VoteSet should match the calculated voting power
	if voteSet.TotalVotingPower != calculatedVotingPower {
		return false
	}

	return true
}

func (voteSet *FnVoteSet) VerifyValidatorSign(validatorIndex int, chainID string, pubKey crypto.PubKey) error {
	if !voteSet.VoteBitArray.GetIndex(validatorIndex) {
		return ErrFnVoteNotPresent
	}

	return voteSet.verifyInternal(voteSet.ValidatorSignatures[validatorIndex], chainID, voteSet.ValidatorAddresses[validatorIndex], pubKey)
}

func (voteSet *FnVoteSet) AddSignature(validatorIndex int, validatorAddress []byte, signature []byte) error {
	if voteSet.VoteBitArray.GetIndex(validatorIndex) {
		return ErrFnVoteAlreadyCasted
	}

	voteSet.ValidatorSignatures[validatorIndex] = signature
	voteSet.ValidatorAddresses[validatorIndex] = validatorAddress
	voteSet.VoteBitArray.SetIndex(validatorIndex, true)

	return nil
}

func (voteSet *FnVoteSet) Merge(anotherSet *FnVoteSet) error {
	if !voteSet.CannonicalCompare(anotherSet) {
		return ErrFnVoteMergeDiffPayload
	}

	numValidators := voteSet.VoteBitArray.Size()

	for i := 0; i < numValidators; i++ {
		if voteSet.VoteBitArray.GetIndex(i) || !anotherSet.VoteBitArray.GetIndex(i) {
			continue
		}

		voteSet.ValidatorSignatures[i] = anotherSet.ValidatorSignatures[i]
		voteSet.ValidatorAddresses[i] = anotherSet.ValidatorAddresses[i]
		voteSet.VoteBitArray.SetIndex(i, true)
	}

	return nil
}

func RegisterFnConsensusTypes() {
	cdc.RegisterConcrete(&FnExecutionRequest{}, "tendermint/fnConsensusReactor/FnExecutionRequest", nil)
	cdc.RegisterConcrete(&FnExecutionResponse{}, "tendermint/fnConsensusReactor/FnExecutionResponse", nil)
	cdc.RegisterConcrete(&FnVoteSet{}, "tendermint/fnConsensusReactor/FnVoteSet", nil)
	cdc.RegisterConcrete(&FnVotePayload{}, "tendermint/fnConsensusReactor/FnVotePayload", nil)
	cdc.RegisterConcrete(&RequestPeerReactorState{}, "tendermint/fnConsensusReactor/RequestPeerReactorState", nil)
	cdc.RegisterConcrete(&ReactorState{}, "tendermint/fnConsensusReactor/ReactorState", nil)
	cdc.RegisterConcrete(&voteSetMarshallable{}, "tendermint/fnConsensusReactor/voteSetMarshallable", nil)
}
