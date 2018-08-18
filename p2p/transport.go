package p2p

import (
	"fmt"
	"net"
	"time"

	"github.com/pkg/errors"

	"github.com/tendermint/tendermint/config"
	crypto "github.com/tendermint/tendermint/crypto"
	cmn "github.com/tendermint/tendermint/libs/common"
	"github.com/tendermint/tendermint/libs/log"
	"github.com/tendermint/tendermint/p2p/conn"
)

const (
	defaultDialTimeout      = time.Second
	defaultFilterTimeout    = 5 * time.Second
	defaultHandshakeTimeout = 3 * time.Second
)

// Transport-level errors.
var (
	ErrTransportFilterTimeout = errors.New("peer filter timeout")
	ErrPeerRejected           = errors.New("peer rejected")
)

// accept is the container to carry the upgraded connection and NodeInfo from an
// asynchronously running routine to the Accept method.
type accept struct {
	conn     net.Conn
	nodeInfo NodeInfo
	err      error
}

// peerConfig is used to bundle data we need to fully setup a Peer with an
// MConn, provided by the caller of Accept and Dial.
type peerConfig struct {
	chDescs              []*conn.ChannelDescriptor
	onPeerError          func(Peer, interface{})
	outbound, persistent bool
	reactorsByCh         map[byte]Reactor
}

// Transport emits and connects to Peers. The implementation of Peer is left to
// the transport. Each transport is also responsible to filter establishing
// peers specific to its domain.
type Transport interface {
	// Accept returns a newly connected Peer.
	Accept(peerConfig) (Peer, error)

	// Dial connects to the Peer for the address.
	Dial(NetAddress, peerConfig) (Peer, error)
}

// transportLifecycle bundles the methods for callers to control start and stop
// behaviour.
type transportLifecycle interface {
	Close() error
	Listen(NetAddress) error
}

type addrFilterFunc func(net.Addr) error
type idFilterFunc func(ID) error

// MultiplexTransportOption sets an optional parameter on the
// MultiplexTransport.
type MultiplexTransportOption func(*MultiplexTransport)

// MultiplexTransportAddrFilter sets the filter function for rejection of peers
// based on their address.
func MultiplexTransportAddrFilter(f addrFilterFunc) MultiplexTransportOption {
	return func(mt *MultiplexTransport) { mt.addrFilter = f }
}

// MultiplexTransportFilterTimeout sets the timeout waited for filter calls to
// return.
func MultiplexTransportFilterTimeout(
	timeout time.Duration,
) MultiplexTransportOption {
	return func(mt *MultiplexTransport) { mt.filterTimeout = timeout }
}

// MultiplexTransportIDFilter sets the filter function for rejection of peers
// based on their ID.
func MultiplexTransportIDFilter(f idFilterFunc) MultiplexTransportOption {
	return func(mt *MultiplexTransport) { mt.idFilter = f }
}

// MultiplexTransport accepts and dials tcp connections and upgrades them to
// multiplexed peers.
type MultiplexTransport struct {
	listener net.Listener

	acceptc chan accept
	closec  chan struct{}

	// Lookup table for duplicate ip and id checks.
	peers map[net.Conn]NodeInfo

	dialTimeout      time.Duration
	filterTimeout    time.Duration
	handshakeTimeout time.Duration
	nodeAddr         NetAddress
	nodeInfo         NodeInfo
	nodeKey          NodeKey

	addrFilter addrFilterFunc
	idFilter   idFilterFunc

	// TODO(xla): Those configs are still needed as we parameterise peerConn and
	// peer currently. All relevant configuration should be refactored into options
	// with sane defaults.
	mConfig   conn.MConnConfig
	p2pConfig config.P2PConfig
}

// Test multiplexTransport for interface completeness.
var _ Transport = (*MultiplexTransport)(nil)
var _ transportLifecycle = (*MultiplexTransport)(nil)

// NewMultiplexTransport returns a tcp conencted multiplexed peers.
func NewMultiplexTransport(
	nodeInfo NodeInfo,
	nodeKey NodeKey,
) *MultiplexTransport {
	return &MultiplexTransport{
		acceptc:          make(chan accept),
		closec:           make(chan struct{}),
		dialTimeout:      defaultDialTimeout,
		filterTimeout:    defaultFilterTimeout,
		handshakeTimeout: defaultHandshakeTimeout,
		mConfig:          conn.DefaultMConnConfig(),
		nodeInfo:         nodeInfo,
		nodeKey:          nodeKey,
		peers:            map[net.Conn]NodeInfo{},
	}
}

// Accept implements Transport.
func (mt *MultiplexTransport) Accept(cfg peerConfig) (Peer, error) {
	select {
	// This case should never have any side-effectful/blocking operations to
	// ensure that quality peers are ready to be used.
	case a := <-mt.acceptc:
		if a.err != nil {
			return nil, a.err
		}

		cfg.outbound = false

		return mt.wrapPeer(a.conn, a.nodeInfo, cfg), nil
	case <-mt.closec:
		return nil, errors.New("transport is closed")
	}
}

// Dial implements Transport.
func (mt *MultiplexTransport) Dial(
	addr NetAddress,
	cfg peerConfig,
) (Peer, error) {
	c, err := addr.DialTimeout(mt.dialTimeout)
	if err != nil {
		return nil, err
	}

	sc, ni, err := mt.upgrade(c)
	if err != nil {
		return nil, err
	}

	cfg.outbound = true

	return mt.wrapPeer(sc, ni, cfg), nil
}

// Close implements transportLifecycle.
func (mt *MultiplexTransport) Close() error {
	close(mt.closec)

	return mt.listener.Close()
}

// Listen implements transportLifecycle.
func (mt *MultiplexTransport) Listen(addr NetAddress) error {
	ln, err := net.Listen("tcp", addr.DialString())
	if err != nil {
		return err
	}

	mt.listener = ln
	mt.nodeAddr = addr

	go mt.acceptPeers()

	return nil
}

func (mt *MultiplexTransport) acceptPeers() {
	for {
		c, err := mt.listener.Accept()
		if err != nil {
			// Close has been called so we silently exit.
			if _, ok := <-mt.closec; !ok {
				return
			}

			mt.acceptc <- accept{err: err}
			return
		}

		// Connection upgrade and filtering should be asynchronous to avoid
		// Head-of-line blocking[0].
		// Reference:  https://github.com/tendermint/tendermint/issues/2047
		//
		// [0] https://en.wikipedia.org/wiki/Head-of-line_blocking
		go func(conn net.Conn) {
			sc, ni, err := mt.upgrade(c)

			select {
			case mt.acceptc <- accept{sc, ni, err}:
				// Make the upgraded peer available.
			case <-mt.closec:
				// Give up if the transport was closed.
				_ = c.Close()
				return
			}
		}(c)
	}
}

func (mt *MultiplexTransport) filterAddr(c net.Conn) error {
	if mt.addrFilter == nil {
		return nil
	}

	errc := make(chan error)

	go func(c net.Conn, errc chan<- error) {
		errc <- mt.addrFilter(c.RemoteAddr())
	}(c, errc)

	select {
	case err := <-errc:
		return errors.Wrap(ErrPeerRejected, err.Error())
	case <-time.After(mt.filterTimeout):
		return ErrTransportFilterTimeout
	}
}

func (mt *MultiplexTransport) filterID(id ID) error {
	if mt.idFilter == nil {
		return nil
	}

	errc := make(chan error)

	go func(id ID, errc chan<- error) {
		errc <- mt.idFilter(id)
	}(id, errc)

	select {
	case err := <-errc:
		return errors.Wrap(ErrPeerRejected, err.Error())
	case <-time.After(mt.filterTimeout):
		return ErrTransportFilterTimeout
	}
}

func (mt *MultiplexTransport) upgrade(
	c net.Conn,
) (sc *conn.SecretConnection, ni NodeInfo, err error) {
	defer func() {
		if err != nil {
			_ = c.Close()
		}
	}()

	if err := mt.filterAddr(c); err != nil {
		return nil, NodeInfo{}, err
	}

	sc, err = secretConn(c, mt.handshakeTimeout, mt.nodeKey.PrivKey)
	if err != nil {
		return nil, NodeInfo{}, fmt.Errorf("secrect conn failed: %v", err)
	}

	ni, err = handshake(sc, mt.handshakeTimeout, mt.nodeInfo)
	if err != nil {
		return nil, NodeInfo{}, fmt.Errorf("handshake failed: %v", err)
	}

	// Ensure connection key matches self reported key.
	if connID := PubKeyToID(sc.RemotePubKey()); connID != ni.ID {
		return nil, NodeInfo{}, errors.Wrapf(
			ErrPeerRejected,
			"conn.ID (%v) NodeInfo.ID (%v) missmatch",
			connID,
			ni.ID,
		)
	}

	if err := mt.filterID(ni.ID); err != nil {
		return nil, NodeInfo{}, err
	}
	// TODO(xla): Check NodeInfo compatibility.

	return sc, ni, nil
}

func (mt *MultiplexTransport) wrapPeer(
	c net.Conn,
	ni NodeInfo,
	cfg peerConfig,
) Peer {
	pc := peerConn{
		conn:       c,
		config:     &mt.p2pConfig,
		outbound:   cfg.outbound,
		persistent: cfg.persistent,
	}

	mt.peers[c] = ni

	return newMultiplexPeer(
		newPeer(
			pc,
			mt.mConfig,
			ni,
			cfg.reactorsByCh,
			cfg.chDescs,
			cfg.onPeerError,
		),
		func() {
			delete(mt.peers, c)
			_ = c.Close()
		},
	)
}

func handshake(
	c net.Conn,
	timeout time.Duration,
	nodeInfo NodeInfo,
) (NodeInfo, error) {
	if err := c.SetDeadline(time.Now().Add(timeout)); err != nil {
		return NodeInfo{}, err
	}

	var (
		errc = make(chan error, 2)

		peerNodeInfo NodeInfo
	)

	go func(errc chan<- error, c net.Conn) {
		_, err := cdc.MarshalBinaryWriter(c, nodeInfo)
		errc <- err
	}(errc, c)
	go func(errc chan<- error, c net.Conn) {
		_, err := cdc.UnmarshalBinaryReader(
			c,
			&peerNodeInfo,
			int64(MaxNodeInfoSize()),
		)
		errc <- err
	}(errc, c)

	for i := 0; i < cap(errc); i++ {
		err := <-errc
		if err != nil {
			return NodeInfo{}, err
		}
	}

	return peerNodeInfo, c.SetDeadline(time.Time{})
}

func secretConn(
	c net.Conn,
	timeout time.Duration,
	privKey crypto.PrivKey,
) (*conn.SecretConnection, error) {
	if err := c.SetDeadline(time.Now().Add(timeout)); err != nil {
		return nil, err
	}

	sc, err := conn.MakeSecretConnection(c, privKey)
	if err != nil {
		return nil, err
	}

	return sc, sc.SetDeadline(time.Time{})
}

// multiplexPeer is the Peer implementation returned by the multiplexTransport
// and wraps the default peer implementation for now, with an added hook for
// shutdown of the peer. It should ultimately grow into the proper
// implementation.
type multiplexPeer struct {
	cmn.BaseService
	*peer

	onStop func()
}

func newMultiplexPeer(p *peer, onStop func()) *multiplexPeer {
	mp := &multiplexPeer{
		onStop: onStop,
		peer:   p,
	}

	mp.BaseService = *cmn.NewBaseService(nil, "MultiplexPeer", mp)

	return mp
}

func (mp *multiplexPeer) OnStart() error {
	return mp.peer.OnStart()
}

func (mp *multiplexPeer) OnStop() {
	mp.onStop()
	mp.peer.OnStop()
}

func (mp *multiplexPeer) SetLogger(logger log.Logger) {
	mp.peer.SetLogger(logger)
}

func (mp *multiplexPeer) String() string {
	return mp.peer.String()
}
