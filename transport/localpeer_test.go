package transport_test

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ironflock/nexus/v3/transport"
	"github.com/ironflock/nexus/v3/wamp"
)

// TestLocalPeerCloseIdempotent verifies that calling Close more than
// once on the same peer is a safe no-op rather than a "close of closed
// channel" panic. The realm shutdown ordering relies on this: a session
// whose handler closed its peer naturally may also be in the snapshot
// of pending-to-close peers that the realm walks at the end of close().
func TestLocalPeerCloseIdempotent(t *testing.T) {
	c, r := transport.LinkedPeers()
	c.Close()
	require.NotPanics(t, func() { c.Close() }, "second Close on client peer must not panic")
	require.NotPanics(t, func() { r.Close() }, "Close on router peer must not panic")
	require.NotPanics(t, func() { r.Close() }, "second Close on router peer must not panic")
}

func TestSendRecv(t *testing.T) {
	c, r := transport.LinkedPeers()

	go func() {
		c.Send() <- &wamp.Hello{}
	}()
	select {
	case <-r.Recv():
	case <-time.After(time.Second):
		require.FailNow(t, "Router peer did not receive msg")
	}

	r.Send() <- &wamp.Welcome{}
	select {
	case <-c.Recv():
	case <-time.After(time.Second):
		require.FailNow(t, "Client peer did not receive msg")
	}

	r.Close()
	select {
	case msg := <-c.Recv():
		require.Nil(t, msg, "Expected nil msg on close")
	case <-time.After(time.Second):
		require.FailNow(t, "Client did not wake up when router closed.")
	}
}

func TestDropOnBlockedClient(t *testing.T) {
	const qsize = 5
	_, r := transport.LinkedPeersQSize(qsize)
	defer r.Close()

	// Effective in-flight capacity is the user-facing Send buffer
	// (qsize) + the forwarder's in-flight slot (1). Fill them, then
	// assert the next non-blocking send drops via the select default.
	const fillTo = qsize + 1
	for range fillTo {
		select {
		case r.Send() <- &wamp.Publish{}:
		case <-time.After(50 * time.Millisecond):
			require.FailNow(t, "fill send should have succeeded")
		}
	}
	done := make(chan struct{})
	var err error
	go func() {
		select {
		case r.Send() <- &wamp.Publish{}:
		default:
			err = errors.New("blocked")
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(50 * time.Millisecond):
		require.FailNow(t, "Send should have dropped and not blocked")
	}
	require.EqualError(t, err, "blocked")
}

func TestBlockOnBlockedRouter(t *testing.T) {
	c, r := transport.LinkedPeers()
	defer c.Close()
	defer r.Close()

	// Effective buffer between c.Send() and r.Recv() is:
	// cap(r.Recv()) (=0, unbuffered) + 1 forwarder in-flight slot.
	// Send `cap+2` so the last send must block (no slot anywhere).
	const extra = 2
	done := make(chan struct{})
	go func() {
		for i := 0; i < cap(r.Recv())+extra; i++ {
			c.Send() <- &wamp.Publish{}
		}
		close(done)
	}()

	select {
	case <-done:
		require.FailNow(t, "Expected send to be blocked")
	case <-time.After(time.Second):
	}
	for i := 0; i < cap(r.Recv())+extra; i++ {
		<-r.Recv()
	}
	<-done
}

// TestLocalPeerSendCloseRaceFree verifies the architectural fix to
// the concurrent-Send-vs-Close race. Even with the race detector on,
// a sender selecting on Send and Done can race Close arbitrarily and
// never trip a panic or a race-detector flag — because the Send
// channel is owned by an internal forwarder, never closed, and the
// forwarder exits via Done.
func TestLocalPeerSendCloseRaceFree(t *testing.T) {
	c, _ := transport.LinkedPeers()

	// Park a sender that uses the cooperative pattern.
	sendDone := make(chan struct{})
	var panicked any
	var observedDone bool
	go func() {
		defer func() {
			panicked = recover()
			close(sendDone)
		}()
		// First send fills the in-flight slot. Second send parks
		// because the partner isn't draining and the forwarder is
		// stuck at its inner write to the partner Recv channel.
		c.Send() <- &wamp.Hello{}
		select {
		case c.Send() <- &wamp.Hello{}:
		case <-c.Done():
			observedDone = true
		}
	}()

	time.Sleep(10 * time.Millisecond) // let sender park

	c.Close()

	select {
	case <-sendDone:
	case <-time.After(time.Second):
		require.FailNow(t, "sender goroutine did not exit after Close")
	}

	require.Nilf(t, panicked, "cooperative Send/Close must not panic, got: %v", panicked)
	require.True(t, observedDone, "sender should have observed Done before Send became ready")
}

// TestLocalPeerDoneClosesOnClose verifies the Peer.Done() contract:
// Close fires the Done channel before unwinding the rest of the
// shutdown sequence, so consumers selecting on Done observe peer
// closure cooperatively.
func TestLocalPeerDoneClosesOnClose(t *testing.T) {
	c, _ := transport.LinkedPeers()

	// Done starts open.
	select {
	case <-c.Done():
		require.FailNow(t, "Done must not be closed before Close")
	default:
	}

	c.Close()

	// Done is closed after Close.
	select {
	case <-c.Done():
	case <-time.After(time.Second):
		require.FailNow(t, "Done must be closed after Close")
	}
}

// TestLocalPeerDoneIndependentPerSide verifies the two peers in a
// LinkedPeers pair have independent Done signals — closing one does
// not signal the other.
func TestLocalPeerDoneIndependentPerSide(t *testing.T) {
	c, r := transport.LinkedPeers()
	c.Close()

	select {
	case <-c.Done():
	default:
		require.FailNow(t, "client peer Done must be closed after client Close")
	}

	select {
	case <-r.Done():
		require.FailNow(t, "router peer Done must not be closed by client Close")
	default:
	}

	r.Close()
	select {
	case <-r.Done():
	default:
		require.FailNow(t, "router peer Done must be closed after router Close")
	}
}

func BenchmarkClientToRouter(b *testing.B) {
	c, r := transport.LinkedPeers()

	b.ResetTimer()
	go func() {
		for i := 0; i < b.N; i++ {
			c.Send() <- &wamp.Hello{}
		}
	}()
	for i := 0; i < b.N; i++ {
		<-r.Recv()
	}
}

func BenchmarkRouterToClient(b *testing.B) {
	c, r := transport.LinkedPeers()

	b.ResetTimer()
	go func() {
		for i := 0; i < b.N; i++ {
			r.Send() <- &wamp.Hello{}
		}
	}()
	for i := 0; i < b.N; i++ {
		<-c.Recv()
	}
}
