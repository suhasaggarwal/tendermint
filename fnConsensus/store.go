package fnConsensus

import dbm "github.com/tendermint/tendermint/libs/db"

const ReactorStateKey = "fnConsensusReactor:state"

func LoadReactorState(db dbm.DB) (*ReactorState, error) {
	rectorStateBytes := db.Get([]byte(ReactorStateKey))
	if rectorStateBytes == nil {
		return NewReactorState(0, nil, nil), nil
	}

	persistedRectorState := &ReactorState{}
	if err := persistedRectorState.Unmarshal(rectorStateBytes); err != nil {
		return nil, err
	}
	return persistedRectorState, nil
}
