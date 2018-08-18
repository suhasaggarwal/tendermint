package p2p

import (
	"fmt"
	"net"
	"reflect"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/tendermint/tendermint/crypto/ed25519"
)

func TestMultiplexTransportAddrFilter(t *testing.T) {
	mt := NewMultiplexTransport(
		NodeInfo{},
		NodeKey{
			PrivKey: ed25519.GenPrivKey(),
		},
	)

	MultiplexTransportAddrFilter(func(addr net.Addr) error {
		return fmt.Errorf("rejected")
	})(mt)

	addr, err := NewNetAddressStringWithOptionalID("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	if err := mt.Listen(*addr); err != nil {
		t.Fatal(err)
	}

	errc := make(chan error)

	go func() {
		addr, err := NewNetAddressStringWithOptionalID(mt.listener.Addr().String())
		if err != nil {
			errc <- err
			return
		}

		_, err = addr.Dial()
		if err != nil {
			errc <- err
			return
		}

		close(errc)
	}()

	if err := <-errc; err != nil {
		t.Errorf("connection failed: %v", err)
	}

	if _, err = mt.Accept(peerConfig{}); errors.Cause(err) != ErrPeerRejected {
		t.Errorf("expected ErrPeerReject")
	}
}

func TestMultiplexTransportAcceptDial(t *testing.T) {
	mt := NewMultiplexTransport(
		NodeInfo{},
		NodeKey{
			PrivKey: ed25519.GenPrivKey(),
		},
	)

	addr, err := NewNetAddressStringWithOptionalID("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	if err := mt.Listen(*addr); err != nil {
		t.Fatal(err)
	}

	errc := make(chan error)

	go func() {
		dialer := NewMultiplexTransport(
			NodeInfo{},
			NodeKey{
				PrivKey: ed25519.GenPrivKey(),
			},
		)

		addr, err := NewNetAddressStringWithOptionalID(mt.listener.Addr().String())
		if err != nil {
			errc <- err
			return
		}

		_, err = dialer.Dial(*addr, peerConfig{})
		if err != nil {
			errc <- err
			return
		}

		close(errc)
	}()

	if err := <-errc; err != nil {
		t.Errorf("connection failed: %v", err)
	}

	p, err := mt.Accept(peerConfig{})
	if err != nil {
		t.Fatal(err)
	}

	// Test that we keep track of the Peer.
	if have, want := len(mt.peers), 1; have != want {
		t.Errorf("have %v, want %v", have, want)
	}

	err = p.Start()
	if err != nil {
		t.Fatal(err)
	}

	err = p.Stop()
	if err != nil {
		t.Fatal(err)
	}

	// Test that we successfully removed the peer after its lifecycle is complete.
	if have, want := len(mt.peers), 0; have != want {
		t.Errorf("have %v, want %v", have, want)
	}

	if err := mt.Close(); err != nil {
		t.Errorf("close errored: %v", err)
	}
}

func TestMultiplexTransportAcceptNonBlocking(t *testing.T) {
	mt := NewMultiplexTransport(
		NodeInfo{},
		NodeKey{
			PrivKey: ed25519.GenPrivKey(),
		},
	)

	addr, err := NewNetAddressStringWithOptionalID("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	if err := mt.Listen(*addr); err != nil {
		t.Fatal(err)
	}

	var (
		pv           = ed25519.GenPrivKey()
		fastNodeInfo = NodeInfo{
			ID: PubKeyToID(pv.PubKey()),
		}
		errc  = make(chan error)
		fastc = make(chan struct{})
		slowc = make(chan struct{})
	)

	// Simulate slow Peer.
	go func() {
		addr, err := NewNetAddressStringWithOptionalID(mt.listener.Addr().String())
		if err != nil {
			errc <- err
			return
		}

		c, err := addr.Dial()
		if err != nil {
			errc <- err
			return
		}

		close(slowc)

		select {
		case <-fastc:
			// Fast peer connected.
		case <-time.After(50 * time.Millisecond):
			// We error if the fast peer didn't succeed.
			errc <- fmt.Errorf("Fast peer timed out")
		}

		sc, err := secretConn(c, 20*time.Millisecond, ed25519.GenPrivKey())
		if err != nil {
			errc <- err
			return
		}

		_, err = handshake(sc, 20*time.Millisecond, NodeInfo{})
		if err != nil {
			errc <- err
			return
		}
	}()

	// Simulate fast Peer.
	go func() {
		<-slowc

		var (
			dialer = NewMultiplexTransport(
				fastNodeInfo,
				NodeKey{
					PrivKey: pv,
				},
			)
		)

		addr, err := NewNetAddressStringWithOptionalID(mt.listener.Addr().String())
		if err != nil {
			errc <- err
			return
		}

		_, err = dialer.Dial(*addr, peerConfig{})
		if err != nil {
			errc <- err
			return
		}

		close(errc)
		close(fastc)
	}()

	if err := <-errc; err != nil {
		t.Errorf("connection failed: %v", err)
	}

	p, err := mt.Accept(peerConfig{})
	if err != nil {
		t.Fatal(err)
	}

	if have, want := p.NodeInfo(), fastNodeInfo; !reflect.DeepEqual(have, want) {
		t.Errorf("have %v, want %v", have, want)
	}
}

func TestTransportHandshake(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	var (
		peerPV       = ed25519.GenPrivKey()
		peerNodeInfo = NodeInfo{
			ID: PubKeyToID(peerPV.PubKey()),
		}
	)

	go func() {
		c, err := net.Dial(ln.Addr().Network(), ln.Addr().String())
		if err != nil {
			t.Error(err)
			return
		}

		go func(c net.Conn) {
			_, err := cdc.MarshalBinaryWriter(c, peerNodeInfo)
			if err != nil {
				t.Error(err)
			}
		}(c)
		go func(c net.Conn) {
			ni := NodeInfo{}

			_, err := cdc.UnmarshalBinaryReader(
				c,
				&ni,
				int64(MaxNodeInfoSize()),
			)
			if err != nil {
				t.Error(err)
			}
		}(c)
	}()

	c, err := ln.Accept()
	if err != nil {
		t.Fatal(err)
	}

	ni, err := handshake(c, 20*time.Millisecond, NodeInfo{})
	if err != nil {
		t.Fatal(err)
	}

	if have, want := ni, peerNodeInfo; !reflect.DeepEqual(have, want) {
		t.Errorf("have %v, want %v", have, want)
	}
}
