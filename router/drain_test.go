package router //nolint:testpackage

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ironflock/nexus/v3/transport"
	"github.com/ironflock/nexus/v3/wamp"
)

// TestRouterDrainGoodbyesClients verifies Drain sends every client
// GOODBYE wamp.close.system_shutdown, refuses new sessions, leaves the
// realm alive (not closed), is idempotent, and that a subsequent Close
// still completes cleanly.
func TestRouterDrainGoodbyesClients(t *testing.T) {
	r := newTestRouter(t)
	t.Cleanup(func() { r.Close() })

	c1 := testClient(t, r)
	c2 := testClient(t, r)

	r.Drain()

	for i, c := range []*wamp.Session{c1, c2} {
		msg, err := wamp.RecvTimeout(c, time.Second)
		require.NoErrorf(t, err, "client %d got no GOODBYE", i)
		gb, ok := msg.(*wamp.Goodbye)
		require.Truef(t, ok, "client %d: expected GOODBYE, got %T", i, msg)
		require.Equal(t, wamp.ErrSystemShutdown, gb.Reason)
	}

	// New sessions are refused while draining.
	client, server := transport.LinkedPeers()
	t.Cleanup(func() { client.Close() })
	go func() {
		client.Send() <- &wamp.Hello{Realm: testRealm, Details: clientRoles}
	}()
	require.Error(t, r.Attach(server), "attach must be refused on a draining realm")

	// The realm is drained but NOT closed: still present, broker/dealer
	// actors still running (Close below would hang/panic otherwise).
	rl := r.(*router).realms[testRealm]
	require.NotNil(t, rl, "realm removed by drain; should stay alive")
	rl.closeLock.Lock()
	require.True(t, rl.draining)
	require.False(t, rl.closed)
	rl.closeLock.Unlock()

	// Idempotent.
	r.Drain()
}

// TestRouterDrainThenCloseIsClean ensures draining first and then closing
// (the daemon's shutdown order) does not double-kick or deadlock.
func TestRouterDrainThenCloseIsClean(t *testing.T) {
	r := newTestRouter(t)
	c := testClient(t, r)

	r.Drain()
	msg, err := wamp.RecvTimeout(c, time.Second)
	require.NoError(t, err)
	_, ok := msg.(*wamp.Goodbye)
	require.True(t, ok)

	done := make(chan struct{})
	go func() {
		r.Close()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Close after Drain hung")
	}
}

// TestRouterDrainEmptyRealm drains a realm with no clients without error.
func TestRouterDrainEmptyRealm(t *testing.T) {
	r := newTestRouter(t)
	t.Cleanup(func() { r.Close() })
	r.Drain() // must not panic or hang with zero sessions
}

// TestRouterDrainClosesIgnoringClientPeer simulates a client that
// receives the system_shutdown GOODBYE but keeps its connection open
// (does not close its peer). close() must reap that peer so its
// transport goroutines do not leak.
func TestRouterDrainClosesIgnoringClientPeer(t *testing.T) {
	r := newTestRouter(t)
	c := testClient(t, r)

	r.Drain()
	msg, err := wamp.RecvTimeout(c, time.Second)
	require.NoError(t, err)
	_, ok := msg.(*wamp.Goodbye)
	require.True(t, ok, "expected GOODBYE, got %T", msg)

	// The "client" deliberately does not close its peer. Closing the
	// router must close the drained-but-open peer.
	r.Close()
	_, err = wamp.RecvTimeout(c, 2*time.Second)
	require.Error(t, err, "drained client peer not closed by Close(): recv still open")
}
