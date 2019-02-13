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
	CurrentVoteSet *FnVoteSet
}

func (p *ReactorState) Marshal() ([]byte, error) {
	return cdc.MarshalBinaryLengthPrefixed(p)
}

func (p *ReactorState) Unmarshal(bz []byte) error {
	return cdc.UnmarshalBinaryLengthPrefixed(bz, p)
}

func NewReactorState(nonce int64, payload *FnVotePayload, valSet *types.ValidatorSet) *ReactorState {
	return &ReactorState{
		CurrentVoteSet: nil,
	}
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

func (f *FnExecutionRequest) Compare(remoteRequest *FnExecutionRequest) bool {
	return f.FnID != remoteRequest.FnID
}

type FnExecutionResponse struct {
	Status int64
	Error  string
	Hash   []byte
}

func (f *FnExecutionResponse) Marshal() ([]byte, error) {
	return cdc.MarshalBinaryLengthPrefixed(f)
}

func (f *FnExecutionResponse) Unmarshal(bz []byte) error {
	return cdc.UnmarshalBinaryLengthPrefixed(bz, f)
}

func (f *FnExecutionResponse) Compare(remoteResponse *FnExecutionResponse) bool {
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

type FnVotePayload struct {
	Request  FnExecutionRequest  `json:"fn_execution_request"`
	Response FnExecutionResponse `json:"fn_execution_response"`
}

func (f *FnVotePayload) Marshal() ([]byte, error) {
	return cdc.MarshalBinaryLengthPrefixed(f)
}

func (f *FnVotePayload) Unmarshal(bz []byte) error {
	return cdc.UnmarshalBinaryLengthPrefixed(bz, f)
}

func (f *FnVotePayload) Compare(remotePayload *FnVotePayload) bool {
	if !f.Request.Compare(&remotePayload.Request) {
		return false
	}

	if !f.Response.Compare(&remotePayload.Response) {
		return false
	}

	return true
}

type FnVoteSet struct {
	Nonce              int64          `json:"nonce"`
	CreationTime       int64          `json:"creation_time"`
	ExpiresIn          int64          `json:"expires_in"`
	VoteBitArray       *cmn.BitArray  `json:"vote_bitarray"`
	Payload            *FnVotePayload `json:"vote_payload"`
	Signatures         [][]byte       `json:"signature"`
	ValidatorAddresses [][]byte       `json:"validator_address"`
}

func NewVoteSet(nonce int64, payload *FnVotePayload, valSet *types.ValidatorSet) *FnVoteSet {
	voteBitArray := cmn.NewBitArray(valSet.Size())
	signatures := make([][]byte, valSet.Size())
	validatorAddresses := make([][]byte, valSet.Size())

	return &FnVoteSet{
		Nonce:              0,
		VoteBitArray:       voteBitArray,
		Signatures:         signatures,
		ValidatorAddresses: validatorAddresses,
		Payload:            payload,
	}
}

func (voteSet *FnVoteSet) Marshal() ([]byte, error) {
	return cdc.MarshalBinaryLengthPrefixed(voteSet)
}

func (voteSet *FnVoteSet) Unmarshal(bz []byte) error {
	return cdc.UnmarshalBinaryLengthPrefixed(bz, voteSet)
}

func (voteSet *FnVoteSet) CannonicalCompare(remoteVoteSet *FnVoteSet) bool {
	if voteSet.Nonce != remoteVoteSet.Nonce {
		return false
	}

	if !voteSet.Payload.Compare(remoteVoteSet.Payload) {
		return false
	}

	if len(voteSet.Signatures) != len(remoteVoteSet.Signatures) {
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

// Should be the first function to be invoked on vote set received from Peer
func (voteSet *FnVoteSet) Meta(chainID string, currentValidatorSet *types.ValidatorSet) *fnVoteSetMeta {
	votesetMeta := &fnVoteSetMeta{
		IsValid:          true,
		IsMaj23:          false,
		TotalVotingPower: 0,
	}

	numValidators := voteSet.VoteBitArray.Size()

	if voteSet.IsExpired() {
		votesetMeta.IsValid = false
		return votesetMeta
	}

	if numValidators != len(voteSet.ValidatorAddresses) {
		votesetMeta.IsValid = false
		return votesetMeta
	}

	if numValidators != len(voteSet.Signatures) {
		votesetMeta.IsValid = false
		return votesetMeta
	}

	if numValidators != currentValidatorSet.Size() {
		votesetMeta.IsValid = false
		return votesetMeta
	}

	currentValidatorSet.Iterate(func(i int, val *types.Validator) bool {
		if !voteSet.VoteBitArray.GetIndex(i) {
			return true
		}
		if err := voteSet.VerifyValidatorSign(i, chainID, val.PubKey); err != nil {
			votesetMeta.IsValid = false
			return false
		}
		votesetMeta.TotalVotingPower += val.VotingPower
		return true
	})

	if votesetMeta.TotalVotingPower >= currentValidatorSet.TotalVotingPower()*2/3+1 {
		votesetMeta.IsMaj23 = true
	}

	return votesetMeta
}

func (voteSet *FnVoteSet) VerifyValidatorSign(validatorIndex int, chainID string, pubKey crypto.PubKey) error {
	if !voteSet.VoteBitArray.GetIndex(validatorIndex) {
		return ErrFnVoteNotPresent
	}

	return voteSet.verifyInternal(voteSet.Signatures[validatorIndex], chainID, voteSet.ValidatorAddresses[validatorIndex], pubKey)
}

func (voteSet *FnVoteSet) AddSignature(validatorIndex int, validatorAddress []byte, signature []byte) error {
	if voteSet.VoteBitArray.GetIndex(validatorIndex) {
		return ErrFnVoteAlreadyCasted
	}

	voteSet.Signatures[validatorIndex] = signature
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

		voteSet.Signatures[i] = anotherSet.Signatures[i]
		voteSet.ValidatorAddresses[i] = anotherSet.Signatures[i]
		voteSet.VoteBitArray.SetIndex(i, true)
	}

	return nil
}

// Internal structure to hold info derived from FnVoteSet
type fnVoteSetMeta struct {
	IsValid          bool
	TotalVotingPower int64
	IsMaj23          bool
}

func RegisterFnConsensusTypes() {
	cdc.RegisterConcrete(&FnExecutionRequest{}, "tendermint/fnConsensusReactor/FnExecutionRequest", nil)
	cdc.RegisterConcrete(&FnExecutionResponse{}, "tendermint/fnConsensusReactor/FnExecutionResponse", nil)
	cdc.RegisterConcrete(&FnVoteSet{}, "tendermint/fnConsensusReactor/FnVoteSet", nil)
	cdc.RegisterConcrete(&FnVotePayload{}, "tendermint/fnConsensusReactor/FnVotePayload", nil)
	cdc.RegisterConcrete(&RequestPeerReactorState{}, "tendermint/fnConsensusReactor/RequestPeerReactorState", nil)
	cdc.RegisterConcrete(&ReactorState{}, "tendermint/fnConsensusReactor/ReactorState", nil)
}
