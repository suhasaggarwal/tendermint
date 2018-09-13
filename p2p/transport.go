package p2p

import (
	"context"
	"fmt"
	"net"
	"sync"
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
)

// IPResolver is a behaviour subset of net.Resolver.
type IPResolver interface {
	LookupIPAddr(context.Context, string) ([]net.IPAddr, error)
}

// accept is the container to carry the upgraded connection and NodeInfo from an
// asynchronously running routine to the Accept method.
type accept struct {
	conn     net.Conn
	nodeInfo NodeInfo
	err      error
}

// peerConfig is used to bundle data we need to fully setup a Peer with an
// MConn, provided by the caller of Accept and Dial (currently the Switch). This
// a temporary measure until reactor setup is less dynamic and we introduce the
// concept of PeerBehaviour to communicate about significant Peer lifecycle
// events.
// TODO(xla): Refactor out with more static Reactor setup and PeerBehaviour.
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

// ConnFilterFunc to be implemented by filter hooks after a new connection has
// been established. The set of exisiting connections is passed along together
// with all resolved IPs for the new connection.
type ConnFilterFunc func(ConnSet, net.Conn, []net.IP) error

// PeerFilterFunc to be implemented by filter hooks after a new Peer has been
// fully setup.
type PeerFilterFunc func(map[ID]Peer, Peer) error

// ConnDuplicateIPFilter resolves and keeps all ips for an incoming connection
// and refuses new ones if they come from a known ip.
func ConnDuplicateIPFilter() ConnFilterFunc {
	return func(cs ConnSet, c net.Conn, ips []net.IP) error {
		for _, ip := range ips {
			if cs.HasIP(ip) {
				return ErrRejected{
					conn:        c,
					err:         fmt.Errorf("IP<%v> already connected", ip),
					isDuplicate: true,
				}
			}
		}

		return nil
	}
}

// MultiplexTransportOption sets an optional parameter on the
// MultiplexTransport.
type MultiplexTransportOption func(*MultiplexTransport)

// MultiplexTransportConnFilters sets the filters for rejection new connections.
func MultiplexTransportConnFilters(
	filters ...ConnFilterFunc,
) MultiplexTransportOption {
	return func(mt *MultiplexTransport) { mt.connFilters = filters }
}

// MultiplexTransportFilterTimeout sets the timeout waited for filter calls to
// return.
func MultiplexTransportFilterTimeout(
	timeout time.Duration,
) MultiplexTransportOption {
	return func(mt *MultiplexTransport) { mt.filterTimeout = timeout }
}

// MultiplexTransportPeerFilters sets the filters for rejection of new peers.
func MultiplexTransportPeerFilters(
	filters ...PeerFilterFunc,
) MultiplexTransportOption {
	return func(mt *MultiplexTransport) { mt.peerFilters = filters }
}

// MultiplexTransportResolver sets the Resolver used for ip lokkups, defaults to
// net.DefaultResolver.
func MultiplexTransportResolver(resolver IPResolver) MultiplexTransportOption {
	return func(mt *MultiplexTransport) { mt.resolver = resolver }
}

// MultiplexTransport accepts and dials tcp connections and upgrades them to
// multiplexed peers.
type MultiplexTransport struct {
	listener net.Listener

	acceptc chan accept
	closec  chan struct{}

	// Lookup table for duplicate ip and id checks.
	conns       ConnSet
	connFilters []ConnFilterFunc

	// TODO(xla): Use PeerSet.
	peers       map[ID]Peer
	peersMux    sync.RWMutex
	peerFilters []PeerFilterFunc

	dialTimeout      time.Duration
	filterTimeout    time.Duration
	handshakeTimeout time.Duration
	nodeAddr         NetAddress
	nodeInfo         NodeInfo
	nodeKey          NodeKey
	resolver         IPResolver

	// TODO(xla): Those configs are still needed as we parameterise peerConn and
	// peer currently. All relevant configuration should be refactored into options
	// with sane defaults.
	mConfig   conn.MConnConfig
	p2pConfig config.P2PConfig
}

// Test multiplexTransport for interface completeness.
var _ Transport = (*MultiplexTransport)(nil)
var _ transportLifecycle = (*MultiplexTransport)(nil)

// NewMultiplexTransport returns a tcp connected multiplexed peer.
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
		conns:            NewConnSet(),
		peers:            map[ID]Peer{},
		resolver:         net.DefaultResolver,
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

		p := mt.wrapPeer(a.conn, a.nodeInfo, cfg)

		return p, mt.filterPeer(p)
	case <-mt.closec:
		return nil, &ErrTransportClosed{}
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

	// TODO(xla): Evaluate if we should apply filters if we explicitly dial.
	if err := mt.filterConn(c); err != nil {
		return nil, err
	}

	sc, ni, err := mt.upgrade(c)
	if err != nil {
		return nil, err
	}

	cfg.outbound = true

	p := mt.wrapPeer(sc, ni, cfg)

	return p, mt.filterPeer(p)
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
			// If Close() has been called, silently exit.
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
		go func(c net.Conn) {
			var (
				ni NodeInfo
				sc *conn.SecretConnection
			)

			err := mt.filterConn(c)
			if err == nil {
				sc, ni, err = mt.upgrade(c)
			}

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

func (mt *MultiplexTransport) cleanup(c net.Conn, id ID) error {
	mt.conns.Remove(c)

	mt.peersMux.Lock()
	delete(mt.peers, id)
	mt.peersMux.Unlock()

	return c.Close()
}

func (mt *MultiplexTransport) filterConn(c net.Conn) error {
	// Reject if connection is already present.
	if mt.conns.Has(c) {
		return ErrRejected{conn: c, isDuplicate: true}
	}

	// Resolve ips for incoming conn.
	ips, err := resolveIPs(mt.resolver, c)
	if err != nil {
		return err
	}

	errc := make(chan error, len(mt.connFilters))

	for _, f := range mt.connFilters {
		go func(f ConnFilterFunc, c net.Conn, ips []net.IP, errc chan<- error) {
			errc <- f(mt.conns, c, ips)
		}(f, c, ips, errc)
	}

	for i := 0; i < cap(errc); i++ {
		select {
		case err := <-errc:
			if err != nil {
				return ErrRejected{conn: c, err: err, isFiltered: true}
			}
		case <-time.After(mt.filterTimeout):
			return ErrTransportFilterTimeout
		}

	}

	mt.conns.Set(c, ips)

	return nil
}

func (mt *MultiplexTransport) filterPeer(p Peer) error {
	// We acquire a single read lock for all filters.
	// XXX(xla): Replace locking madness with concurrent datastructures (PeerSet).
	mt.peersMux.RLock()

	if _, ok := mt.peers[p.ID()]; ok {
		return ErrRejected{id: p.ID(), isDuplicate: true}
	}

	for _, f := range mt.peerFilters {
		errc := make(chan error)

		go func(f PeerFilterFunc, p Peer, errc chan<- error) {
			errc <- f(mt.peers, p)
		}(f, p, errc)

		select {
		case err := <-errc:
			return ErrRejected{err: err, id: p.ID(), isFiltered: true}
		case <-time.After(mt.filterTimeout):
			return ErrTransportFilterTimeout
		}
	}

	mt.peersMux.RUnlock()

	mt.peersMux.Lock()
	mt.peers[p.ID()] = p
	mt.peersMux.Unlock()

	return nil
}

func (mt *MultiplexTransport) upgrade(
	c net.Conn,
) (sc *conn.SecretConnection, ni NodeInfo, err error) {
	defer func() {
		if err != nil {
			_ = mt.cleanup(c, ni.ID)
		}
	}()

	sc, err = secretConn(c, mt.handshakeTimeout, mt.nodeKey.PrivKey)
	if err != nil {
		return nil, NodeInfo{}, ErrRejected{
			conn:          c,
			err:           fmt.Errorf("secrect conn failed: %v", err),
			isAuthFailure: true,
		}
	}

	ni, err = handshake(sc, mt.handshakeTimeout, mt.nodeInfo)
	if err != nil {
		return nil, NodeInfo{}, ErrRejected{
			conn:          c,
			err:           fmt.Errorf("handshake failed: %v", err),
			isAuthFailure: true,
		}
	}

	if err := ni.Validate(); err != nil {
		return nil, NodeInfo{}, ErrRejected{
			conn:              c,
			err:               err,
			isNodeInfoInvalid: true,
		}
	}

	// Ensure connection key matches self reported key.
	if connID := PubKeyToID(sc.RemotePubKey()); connID != ni.ID {
		return nil, NodeInfo{}, ErrRejected{
			conn: c,
			id:   connID,
			err: fmt.Errorf(
				"conn.ID (%v) NodeInfo.ID (%v) missmatch",
				connID,
				ni.ID,
			),
			isAuthFailure: true,
		}
	}

	// Reject self.
	if mt.nodeInfo.ID == ni.ID {
		return nil, NodeInfo{}, ErrRejected{conn: c, id: ni.ID, isSelf: true}
	}

	if err := mt.nodeInfo.CompatibleWith(ni); err != nil {
		return nil, NodeInfo{}, ErrRejected{
			conn:           c,
			err:            err,
			id:             ni.ID,
			isIncompatible: true,
		}
	}

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
			_ = mt.cleanup(c, ni.ID)
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

func resolveIPs(resolver IPResolver, c net.Conn) ([]net.IP, error) {
	host, _, err := net.SplitHostPort(c.RemoteAddr().String())
	if err != nil {
		return nil, err
	}

	addrs, err := resolver.LookupIPAddr(context.Background(), host)
	if err != nil {
		return nil, err
	}

	ips := []net.IP{}

	for _, addr := range addrs {
		ips = append(ips, addr.IP)
	}

	return ips, nil
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
