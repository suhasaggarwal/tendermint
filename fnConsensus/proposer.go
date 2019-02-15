package fnConsensus

type FnProposer interface {
	Propose(fnID string)
	ProposeChannel() <-chan string
}

type SimpleFnProposer struct {
	proposeCh chan string
}

func NewSimpleFnProposer(proposerBuffer int) *SimpleFnProposer {
	return &SimpleFnProposer{
		proposeCh: make(chan string, proposerBuffer),
	}
}

func (s *SimpleFnProposer) Propose(fnID string) {
	s.proposeCh <- fnID
}

func (s *SimpleFnProposer) ProposeChannel() <-chan string {
	return s.proposeCh
}
