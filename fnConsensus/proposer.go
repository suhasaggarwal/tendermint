package fnConsensus

type FnProposer struct {
	proposeCh chan string
}

func NewFnProposer(proposerBuffer int) *FnProposer {
	return &FnProposer{
		proposeCh: make(chan string, proposerBuffer),
	}
}

func (p *FnProposer) Propose(fnID string) {
	p.proposeCh <- fnID
}

func (p *FnProposer) ProposeChannel() <-chan string {
	return p.proposeCh
}
