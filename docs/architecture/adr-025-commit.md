# ADR 025 Commit

## Context
Currently the `Commit` structure contains a lot of potentially redundant or unnecessary data.
In particular it contains an array of every precommit from the validators, which includes many copies of the same data. Such as `Height`, `Round`, `Type`, and `BlockID`. Also the `ValidatorIndex` could be derived from the vote's position in the array, and the `ValidatorAddress` could potentially be derived from runtime context. The only truely necessary data is the `Signature` and `Timestamp` associated with each `Vote`.

```
type Commit struct {
    BlockID    BlockID `json:"block_id"`
    Precommits []*Vote `json:"precommits"`
}
type Vote struct {
    ValidatorAddress Address   `json:"validator_address"`
    ValidatorIndex   int       `json:"validator_index"`
    Height           int64     `json:"height"`
    Round            int       `json:"round"`
    Timestamp        time.Time `json:"timestamp"`
    Type             byte      `json:"type"`
    BlockID          BlockID   `json:"block_id"`
    Signature        []byte    `json:"signature"`
}
```
References:
[#1648](https://github.com/tendermint/tendermint/issues/1648)
[#2179](https://github.com/tendermint/tendermint/issues/2179)
[#2226](https://github.com/tendermint/tendermint/issues/2226)

## Decision
We can improve efficiency by replacing the usage of the `Vote` struct with a subset of each vote, and by storing the constant values (`Height`, `Round`, `BlockID`) in the Commit itself.
```
type Commit struct {
    Height  int64
    Round   int
    BlockID    BlockID      `json:"block_id"`
    Precommits []*CommitSig `json:"precommits"`
}
type CommitSig struct {
    ValidatorAddress Address
    Signature []byte
    Timestamp time.Time
}
```

## Status
Draft - WIP

## Consequences

### Positive
The size of a `Commit` transmitted over the network goes from:

|BlockID| + n * (|Address| + |ValidatorIndex| + |Height| + |Round| + |Timestamp| + |Type| + |BlockID| + |Signature|)

to:


|BlockID|+|Height|+|Round| + n*(|Address| + |Signature| + |Timestamp|)

This saves:

n * (|BlockID| + |ValidatorIndex| + |Type|) + (n-1) * (Height + Round)

In the current context, this would concretely be:
(assuming all ints are int64, and hashes are 32 bytes)

n *(72 + 8 + 1 + 8 + 8) - 16 = n * 97 - 16

With 100 validators this is a savings of almost 10KB on every block.



### Negative

### Neutral
