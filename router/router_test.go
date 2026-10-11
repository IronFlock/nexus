package router //nolint:testpackage

import (
	"log"
	"os"
	"slices"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ironflock/nexus/v3/stdlog"
	"github.com/ironflock/nexus/v3/transport"
	"github.com/ironflock/nexus/v3/wamp"
)

const (
	testRealm       = wamp.URI("nexus.test.realm")
	testRealm2      = wamp.URI("nexus.test.realm2")
	testProcedure   = wamp.URI("nexus.test.endpoint")
	testProcedureWC = wamp.URI("nexus..endpoint")
	testTopic       = wamp.URI("nexus.test.event")
	testTopicPfx    = wamp.URI("nexus.test")
	testTopicWC     = wamp.URI("nexus..event")
)

var (
	debug  bool
	logger stdlog.StdLog
)

func init() {
	debug = false
	logger = log.New(os.Stdout, "", log.LstdFlags)
}

var clientRoles = wamp.Dict{
	"roles": wamp.Dict{
		"subscriber": wamp.Dict{
			"features": wamp.Dict{
				"publisher_identification": true,
			},
		},
		"publisher": wamp.Dict{
			"features": wamp.Dict{
				"subscriber_blackwhite_listing": true,
			},
		},
		"callee": wamp.Dict{},
		"caller": wamp.Dict{
			"features": wamp.Dict{
				"call_timeout": true,
			},
		},
	},
	"authmethods": wamp.List{"anonymous", "ticket"},
}

func newTestRouter(t *testing.T) Router {
	config := &Config{
		RealmConfigs: []*RealmConfig{
			{
				URI:              testRealm,
				StrictURI:        false,
				AnonymousAuth:    true,
				AllowDisclose:    false,
				EnableMetaKill:   true,
				EnableMetaModify: true,
			},
		},
		Debug: debug,
	}

	r, err := NewRouter(config, logger)
	require.NoError(t, err)
	t.Cleanup(func() {
		r.Close()
	})
	return r
}

func testClientInRealm(t *testing.T, r Router, realm wamp.URI) *wamp.Session {
	client, server := transport.LinkedPeers()
	t.Cleanup(func() { client.Close() })
	// Run as goroutine since Send will block until message read by router, if
	// client uses unbuffered channel.
	details := clientRoles
	details["authid"] = "user1"
	details["xyzzy"] = "plugh"
	go func() {
		client.Send() <- &wamp.Hello{Realm: realm, Details: details}
	}()
	err := r.Attach(server)
	require.NoError(t, err)

	msg, err := wamp.RecvTimeout(client, time.Second)
	require.NoError(t, err, "error waiting for welcome")
	welcome, ok := msg.(*wamp.Welcome)
	require.True(t, ok, "expected WELCOME")

	return &wamp.Session{
		Peer:    client,
		ID:      welcome.ID,
		Details: welcome.Details,
	}
}

func testClient(t *testing.T, r Router) *wamp.Session {
	return testClientInRealm(t, r, testRealm)
}

func TestHandshake(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := newTestRouter(t)
		defer r.Close()

		cli := testClient(t, r)
		cli.Send() <- &wamp.Goodbye{}
		msg, err := wamp.RecvTimeout(cli, time.Second)
		require.NoError(t, err, "no goodbye message after sending goodbye")
		_, ok := msg.(*wamp.Goodbye)
		require.True(t, ok, "expected GOODBYE")
	})
}

func TestHandshakeBadRealm(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := newTestRouter(t)
		defer r.Close()
		client, server := transport.LinkedPeers()
		go func() {
			client.Send() <- &wamp.Hello{Realm: "does.not.exist"}
		}()
		err := r.Attach(server)
		require.Error(t, err)

		msg, err := wamp.RecvTimeout(client, time.Second)
		require.NoError(t, err, "timed out waiting for response to HELLO")
		_, ok := msg.(*wamp.Abort)
		require.True(t, ok, "Expected ABORT after bad handshake")
	})
}

// Test for protocol violation.
func TestProtocolViolation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := newTestRouter(t)
		defer r.Close()
		cli := testClient(t, r)

		// Send HELLO message after session established.
		cli.Send() <- &wamp.Hello{Realm: testRealm, Details: clientRoles}
		msg, err := wamp.RecvTimeout(cli, time.Second)
		require.NoError(t, err, "timed out waiting for ABORT")
		abort, ok := msg.(*wamp.Abort)
		require.True(t, ok, "expected ABORT")
		require.Equal(t, wamp.ErrProtocolViolation, abort.Reason)

		// Send SUBSCRIBE before session established.
		client, server := transport.LinkedPeers()
		// Run as goroutine since Send will block until message read by router, if
		// client uses unbuffered channel.
		go func() {
			client.Send() <- &wamp.Subscribe{Request: wamp.GlobalID(), Topic: wamp.URI("some.uri")}
		}()
		err = r.Attach(server)
		require.Error(t, err)

		synctest.Wait()
		msg, err = wamp.RecvTimeout(client, time.Second)
		require.NoError(t, err, "timed out waiting for ABORT")
		abort, ok = msg.(*wamp.Abort)
		require.True(t, ok, "expected ABORT")
		require.Equal(t, wamp.ErrProtocolViolation, abort.Reason)
	})
}

// newStrictIDsTestRouter creates a router with a single realm that has
// StrictRequestIDs enabled, for tests that exercise the WAMP §5.1.2
// request-ID sequence requirement.
func newStrictIDsTestRouter(t *testing.T) Router {
	t.Helper()
	config := &Config{
		RealmConfigs: []*RealmConfig{
			{
				URI:              testRealm,
				StrictURI:        false,
				AnonymousAuth:    true,
				StrictRequestIDs: true,
			},
		},
		Debug: debug,
	}
	r, err := NewRouter(config, logger)
	require.NoError(t, err)
	t.Cleanup(func() { r.Close() })
	return r
}

// TestStrictRequestIDsAccepted pins gammazero/nexus#293: with
// StrictRequestIDs=true, a client whose request IDs start at 1 and
// increment by 1 must be accepted normally.
func TestStrictRequestIDsAccepted(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := newStrictIDsTestRouter(t)
		defer r.Close()
		cli := testClient(t, r)

		// First request: id=1.
		cli.Send() <- &wamp.Subscribe{Request: 1, Topic: testTopic}
		msg, err := wamp.RecvTimeout(cli, time.Second)
		require.NoError(t, err, "no SUBSCRIBED for id=1")
		_, ok := msg.(*wamp.Subscribed)
		require.True(t, ok, "expected SUBSCRIBED for in-sequence id=1, got %T", msg)

		// Second request: id=2.
		cli.Send() <- &wamp.Subscribe{Request: 2, Topic: testTopic + ".other"}
		msg, err = wamp.RecvTimeout(cli, time.Second)
		require.NoError(t, err, "no SUBSCRIBED for id=2")
		_, ok = msg.(*wamp.Subscribed)
		require.True(t, ok, "expected SUBSCRIBED for in-sequence id=2, got %T", msg)
	})
}

// TestStrictRequestIDsRejectsOutOfSequence verifies that a
// non-sequential request ID triggers ABORT(wamp.error.protocol_violation)
// per WAMP §5.1.2.
func TestStrictRequestIDsRejectsOutOfSequence(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := newStrictIDsTestRouter(t)
		defer r.Close()
		cli := testClient(t, r)

		// Skip id=1, start at 99 — protocol violation.
		cli.Send() <- &wamp.Subscribe{Request: 99, Topic: testTopic}
		msg, err := wamp.RecvTimeout(cli, time.Second)
		require.NoError(t, err, "no ABORT after non-sequential id")
		abort, ok := msg.(*wamp.Abort)
		require.True(t, ok, "expected ABORT, got %T", msg)
		require.Equal(t, wamp.ErrProtocolViolation, abort.Reason)
	})
}

// TestStrictRequestIDsRejectsGap verifies a gap mid-sequence (1, 2, 4)
// also triggers ABORT.
func TestStrictRequestIDsRejectsGap(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := newStrictIDsTestRouter(t)
		defer r.Close()
		cli := testClient(t, r)

		cli.Send() <- &wamp.Subscribe{Request: 1, Topic: testTopic}
		_, err := wamp.RecvTimeout(cli, time.Second)
		require.NoError(t, err)

		cli.Send() <- &wamp.Subscribe{Request: 2, Topic: testTopic + ".b"}
		_, err = wamp.RecvTimeout(cli, time.Second)
		require.NoError(t, err)

		// Skip id=3, jump to 4 — protocol violation.
		cli.Send() <- &wamp.Subscribe{Request: 4, Topic: testTopic + ".c"}
		msg, err := wamp.RecvTimeout(cli, time.Second)
		require.NoError(t, err)
		abort, ok := msg.(*wamp.Abort)
		require.True(t, ok, "expected ABORT after gap, got %T", msg)
		require.Equal(t, wamp.ErrProtocolViolation, abort.Reason)
	})
}

// TestStrictRequestIDsDisabledByDefault verifies the existing relaxed
// behavior is preserved: with StrictRequestIDs=false (the default),
// any request ID — including random ones — is accepted.
func TestStrictRequestIDsDisabledByDefault(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := newTestRouter(t) // no StrictRequestIDs
		cli := testClient(t, r)

		// Random ID, not 1: should still work in default mode.
		cli.Send() <- &wamp.Subscribe{Request: 99999, Topic: testTopic}
		msg, err := wamp.RecvTimeout(cli, time.Second)
		require.NoError(t, err)
		_, ok := msg.(*wamp.Subscribed)
		require.True(t, ok, "default mode must still accept arbitrary ids; got %T", msg)
	})
}

// TestRealmShutdownDrainsActorsBeforePeerClose pins the five-stage
// realm.close ordering. Before this fix, broker/dealer actor goroutines
// stayed alive while session-handler goroutines exited and closed their
// peers — an in-flight trySend from the actor could race against the
// peer's chan-close, panicking the router (caught by trySend's
// defer/recover but flagged by the race detector and structurally wrong).
//
// The scenario:
//   - Caller with a 1-slot outbound queue. After WELCOME, the queue is
//     left to fill on its own — the test never drains it.
//   - Two CALLs to a callee. Callee returns INVOCATION ERROR for each.
//     The dealer's syncError calls trySend(caller, errMsg); the first
//     fits in caller's queue, the second drops via select-default.
//   - Test ends, t.Cleanup runs r.Close(). With the fix, dealer.close
//     and broker.close are awaited BEFORE caller's peer is closed, so
//     no actor can be in trySend when the peer closes.
//
// Verified clean under `go test -race` after the fix; without the fix
// the same scenario reliably trips the race detector and (without
// trySend's defer/recover) crashes with `send on closed channel`.
func TestRealmShutdownDrainsActorsBeforePeerClose(t *testing.T) {
	r := newTestRouter(t)

	// Slow caller: 1-slot router→client queue.
	callerClient, callerServer := transport.LinkedPeersQSize(1)
	go func() {
		callerClient.Send() <- &wamp.Hello{Realm: testRealm, Details: clientRoles}
	}()
	require.NoError(t, r.Attach(callerServer))
	msg, err := wamp.RecvTimeout(callerClient, time.Second)
	require.NoError(t, err)
	_, ok := msg.(*wamp.Welcome)
	require.True(t, ok, "expected WELCOME, got %T", msg)

	callee := testClient(t, r)
	const proc = wamp.URI("nexus.test.shutdownrace.proc")
	callee.Send() <- &wamp.Register{Request: 1, Procedure: proc}
	msg, err = wamp.RecvTimeout(callee, time.Second)
	require.NoError(t, err)
	_, ok = msg.(*wamp.Registered)
	require.True(t, ok)

	// Two CALL → INVOCATION ERROR exchanges, leaving caller's queue
	// occupied by the first ERROR with the second drop-logged.
	for i := 1; i <= 2; i++ {
		callerClient.Send() <- &wamp.Call{Request: wamp.ID(i), Procedure: proc}
		msg, err = wamp.RecvTimeout(callee, time.Second)
		require.NoErrorf(t, err, "callee never received INVOCATION %d", i)
		inv, ok := msg.(*wamp.Invocation)
		require.True(t, ok, "expected INVOCATION %d, got %T", i, msg)
		callee.Send() <- &wamp.Error{
			Type: wamp.INVOCATION, Request: inv.Request,
			Error: wamp.ErrInvalidArgument,
		}
	}

	// t.Cleanup runs r.Close at this point — that's what triggers the
	// shutdown ordering under test. The require here just ensures the
	// callee remained responsive end-to-end before cleanup.
	callee.Send() <- &wamp.Subscribe{Request: 100, Topic: testTopic}
	msg, err = wamp.RecvTimeout(callee, time.Second)
	require.NoError(t, err)
	_, ok = msg.(*wamp.Subscribed)
	require.True(t, ok, "callee not responsive before shutdown")
}

func TestRouterSubscribe(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := newTestRouter(t)
		defer r.Close()
		sub := testClient(t, r)

		subscribeID := wamp.GlobalID()
		sub.Send() <- &wamp.Subscribe{Request: subscribeID, Topic: testTopic}
		msg, err := wamp.RecvTimeout(sub, time.Second)
		require.NoError(t, err, "Timed out waiting for SUBSCRIBED")
		subMsg, ok := msg.(*wamp.Subscribed)
		require.True(t, ok, "Expected SUBSCRIBED")
		require.Equal(t, subscribeID, subMsg.Request, "wrong request ID")
		subscriptionID := subMsg.Subscription

		pub := testClient(t, r)
		pubID := wamp.GlobalID()
		pub.Send() <- &wamp.Publish{Request: pubID, Topic: testTopic}

		msg, err = wamp.RecvTimeout(sub, time.Second)
		require.NoError(t, err, "Timed out waiting for EVENT")
		event, ok := msg.(*wamp.Event)
		require.True(t, ok, "Expected EVENT")
		require.Equal(t, subscriptionID, event.Subscription, "wrong subscription ID")
	})
}

func TestPublishAcknowledge(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := newTestRouter(t)
		defer r.Close()
		client := testClient(t, r)

		id := wamp.GlobalID()
		client.Send() <- &wamp.Publish{
			Request: id,
			Options: wamp.Dict{"acknowledge": true},
			Topic:   "some.uri",
		}

		msg, err := wamp.RecvTimeout(client, time.Second)
		require.NoError(t, err, "sent acknowledge=true, timed out waiting for PUBLISHED")
		pub, ok := msg.(*wamp.Published)
		require.Truef(t, ok, "sent acknowledge=true, expected PUBLISHED, got: %s", msg.MessageType())
		require.Equal(t, id, pub.Request, "wrong request id")
	})
}

func TestPublishFalseAcknowledge(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := newTestRouter(t)
		defer r.Close()
		client := testClient(t, r)

		id := wamp.GlobalID()
		client.Send() <- &wamp.Publish{
			Request: id,
			Options: wamp.Dict{"acknowledge": false},
			Topic:   "some.uri",
		}

		synctest.Wait()
		msg, err := wamp.RecvTimeout(client, 200*time.Millisecond)
		if err == nil {
			_, ok := msg.(*wamp.Published)
			require.Falsef(t, ok, "Sent acknowledge=false, but received PUBLISHED: %s", msg.MessageType())
		}
	})
}

func TestPublishNoAcknowledge(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := newTestRouter(t)
		defer r.Close()
		client := testClient(t, r)

		id := wamp.GlobalID()
		client.Send() <- &wamp.Publish{Request: id, Topic: "some.uri"}
		synctest.Wait()
		msg, err := wamp.RecvTimeout(client, 200*time.Millisecond)
		if err == nil {
			_, ok := msg.(*wamp.Published)
			require.Falsef(t, ok, "Sent acknowledge=false, but received PUBLISHED: %s", msg.MessageType())
		}
	})
}

// TestDealerYieldDoesNotBlockCalleeOnSlowCaller pins gammazero/nexus#324:
// when a caller's outbound queue fills (the caller is slow or unresponsive
// reading its Recv channel), the dealer's RESULT-delivery retry loop must
// not stall the callee's session goroutine. Otherwise a single bad caller
// freezes communication for the callee, even though the callee has
// nothing wrong with it.
//
// Without the fix this test trips its 1-second deadline; with the fix the
// callee remains responsive within milliseconds.
func TestDealerYieldDoesNotBlockCalleeOnSlowCaller(t *testing.T) {
	r := newTestRouter(t)

	// Caller with the smallest practical outbound queue. After WELCOME is
	// drained the queue is empty; one stalled RESULT will fill it.
	callerClient, callerServer := transport.LinkedPeersQSize(1)
	go func() {
		callerClient.Send() <- &wamp.Hello{Realm: testRealm, Details: clientRoles}
	}()
	require.NoError(t, r.Attach(callerServer))
	welcome := <-callerClient.Recv()
	_, ok := welcome.(*wamp.Welcome)
	require.True(t, ok, "expected WELCOME, got %T", welcome)

	// Callee with the normal default queue.
	callee := testClient(t, r)

	const proc = wamp.URI("nexus.test.slowcaller.proc")
	callee.Send() <- &wamp.Register{Request: 1, Procedure: proc}
	msg, err := wamp.RecvTimeout(callee, time.Second)
	require.NoError(t, err)
	_, ok = msg.(*wamp.Registered)
	require.True(t, ok, "expected REGISTERED, got %T", msg)

	deliverYield := func(callRequest wamp.ID) {
		t.Helper()
		callerClient.Send() <- &wamp.Call{Request: callRequest, Procedure: proc}
		msg, err := wamp.RecvTimeout(callee, time.Second)
		require.NoErrorf(t, err, "callee never received INVOCATION for call %d", callRequest)
		inv, ok := msg.(*wamp.Invocation)
		require.True(t, ok, "expected INVOCATION, got %T", msg)
		callee.Send() <- &wamp.Yield{Request: inv.Request}
	}

	// First call: RESULT 1 fills caller's 1-slot queue (test never reads it).
	deliverYield(1)

	// Second call: RESULT 2 cannot be queued — caller is now blocked.
	// dealer.yield enters its retry loop. With the bug, this retry runs on
	// the callee's session goroutine and blocks it.
	deliverYield(2)

	// The callee must still be able to do other work while the dealer
	// retries delivering RESULT 2 in the background. SUBSCRIBE → SUBSCRIBED
	// must complete promptly; without the fix it waits for the retry
	// deadline (~1 minute).
	callee.Send() <- &wamp.Subscribe{Request: 100, Topic: testTopic}
	select {
	case msg := <-callee.Recv():
		_, ok := msg.(*wamp.Subscribed)
		require.True(t, ok, "expected SUBSCRIBED, got %T", msg)
	case <-time.After(time.Second):
		t.Fatal("callee blocked for >1s while caller's queue is full — see #324")
	}
}

// unauthSlowPeer attaches a fresh anonymous-auth client to the router with a
// router-to-client outbound queue of `qsize`, drains the WELCOME, and returns
// the client-side peer. Used by tests that need precise control over how
// quickly the test side reads (or doesn't read) router-to-client traffic.
func unauthSlowPeer(t *testing.T, r Router, qsize int) wamp.Peer {
	t.Helper()
	client, server := transport.LinkedPeersQSize(qsize)
	go func() {
		client.Send() <- &wamp.Hello{Realm: testRealm, Details: clientRoles}
	}()
	require.NoError(t, r.Attach(server))
	msg, err := wamp.RecvTimeout(client, time.Second)
	require.NoError(t, err, "no WELCOME on slow peer")
	_, ok := msg.(*wamp.Welcome)
	require.True(t, ok, "expected WELCOME, got %T", msg)
	return client
}

// TestSlowSubscriberDoesNotBlockBroker pins the broker.trySend
// non-blocking semantics: a subscriber whose outbound queue is full
// must NOT stall fan-out to other subscribers. The slow subscriber's
// EVENT is silently dropped (the broker logs a "Dropped EVENT" warning),
// while the fast subscriber receives the event normally.
//
// Adjacent neighbor of #324: same broker fanout, different blocked party.
func TestSlowSubscriberDoesNotBlockBroker(t *testing.T) {
	r := newTestRouter(t)

	// Slow subscriber: 1-slot queue, used up after SUBSCRIBED ack and
	// then never drained again.
	slow := unauthSlowPeer(t, r, 1)
	slow.Send() <- &wamp.Subscribe{Request: 1, Topic: testTopic}
	msg, err := wamp.RecvTimeout(slow, time.Second)
	require.NoError(t, err)
	_, ok := msg.(*wamp.Subscribed)
	require.True(t, ok, "expected SUBSCRIBED on slow peer, got %T", msg)
	// Slow peer no longer drains anything.

	// Fast subscriber: normal queue.
	fast := testClient(t, r)
	fast.Send() <- &wamp.Subscribe{Request: 1, Topic: testTopic}
	msg, err = wamp.RecvTimeout(fast, time.Second)
	require.NoError(t, err)
	_, ok = msg.(*wamp.Subscribed)
	require.True(t, ok)

	// Publisher.
	pub := testClient(t, r)
	pub.Send() <- &wamp.Publish{
		Request: 1, Topic: testTopic,
		Options:   wamp.Dict{wamp.OptAcknowledge: true},
		Arguments: wamp.List{"hi"},
	}

	// Fast subscriber must still receive the EVENT promptly.
	got := false
	for !got {
		select {
		case msg := <-fast.Recv():
			if _, isPublished := msg.(*wamp.Published); isPublished {
				continue // publisher's PUBLISHED ack arrives interleaved
			}
			_, ok := msg.(*wamp.Event)
			require.True(t, ok, "fast subscriber: expected EVENT, got %T", msg)
			got = true
		case <-time.After(time.Second):
			t.Fatal("fast subscriber did not get EVENT — slow subscriber stalled fan-out?")
		}
	}
}

// TestSlowCallerOnInvocationError verifies that when a caller's outbound
// queue is full and the callee returns an INVOCATION ERROR, the dealer's
// non-blocking trySend drops the resulting CALL ERROR rather than
// stalling the callee. Pins existing behavior; ensures the #324 fix
// didn't introduce a parallel deadlock on the error path.
//
// Was previously deferred because it tripped the race detector via the
// realm-shutdown ordering bug; now race-clean after the five-stage
// realm.close fix.
func TestSlowCallerOnInvocationError(t *testing.T) {
	r := newTestRouter(t)

	// Slow caller: 1-slot queue, never drained beyond WELCOME.
	caller := unauthSlowPeer(t, r, 1)

	callee := testClient(t, r)
	const proc = wamp.URI("nexus.test.errproc")
	callee.Send() <- &wamp.Register{Request: 1, Procedure: proc}
	msg, err := wamp.RecvTimeout(callee, time.Second)
	require.NoError(t, err)
	_, ok := msg.(*wamp.Registered)
	require.True(t, ok)

	// Issue two CALLs; for each, callee returns INVOCATION ERROR.
	// The slow caller's queue accepts the first ERROR and then blocks
	// the second (drop via trySend).
	for i := 1; i <= 2; i++ {
		caller.Send() <- &wamp.Call{Request: wamp.ID(i), Procedure: proc}
		msg, err = wamp.RecvTimeout(callee, time.Second)
		require.NoErrorf(t, err, "callee never received INVOCATION %d", i)
		inv, ok := msg.(*wamp.Invocation)
		require.True(t, ok, "expected INVOCATION %d, got %T", i, msg)
		callee.Send() <- &wamp.Error{
			Type: wamp.INVOCATION, Request: inv.Request,
			Error: wamp.ErrInvalidArgument,
		}
	}

	// Sanity: callee remains responsive — neither blocked nor leaked
	// state from the dropped ERROR.
	callee.Send() <- &wamp.Subscribe{Request: 100, Topic: testTopic}
	select {
	case msg := <-callee.Recv():
		_, ok := msg.(*wamp.Subscribed)
		require.True(t, ok, "expected SUBSCRIBED, got %T", msg)
	case <-time.After(time.Second):
		t.Fatal("callee blocked despite slow caller on INVOCATION ERROR path")
	}
}

// TestSlowCalleeRecoverableAfterDrain verifies the dealer's non-blocking
// INVOCATION delivery (`syncCall`) handles a temporarily blocked callee
// gracefully: the caller is told the call failed (network_failure),
// dealer state is cleaned up, and once the callee drains its queue,
// subsequent calls succeed normally.
//
// Adjacent to #324 — caller-side analog of the slow-caller scenario.
// Race-clean after the realm-shutdown ordering fix.
func TestSlowCalleeRecoverableAfterDrain(t *testing.T) {
	r := newTestRouter(t)

	// Callee with 1-slot queue. With the forwarder-based localPeer,
	// the effective r→c buffer is qsize (rOut) + 1 (forwarder
	// in-flight) = 2 slots. So we need to send TWO INVOCATIONs to
	// fill the buffer before the third call hits the dealer's
	// non-blocking trySend default branch.
	callee := unauthSlowPeer(t, r, 1)
	const proc = wamp.URI("nexus.test.slowcallee")
	callee.Send() <- &wamp.Register{Request: 1, Procedure: proc}
	msg, err := wamp.RecvTimeout(callee, time.Second)
	require.NoError(t, err)
	_, ok := msg.(*wamp.Registered)
	require.True(t, ok)

	caller := testClient(t, r)

	// Calls 1 and 2: fill callee's 2-slot effective queue. We
	// intentionally do NOT drain them — keeping the queue full is
	// the whole point of the next assertion.
	caller.Send() <- &wamp.Call{Request: 1, Procedure: proc}
	caller.Send() <- &wamp.Call{Request: 2, Procedure: proc}
	// Brief sleep lets the dealer dispatch both INVOCATIONs onto
	// callee's outbound channel before we issue CALL 3.
	time.Sleep(100 * time.Millisecond)

	// Third call: callee queue is now full. Dealer's syncCall must
	// react with ERROR(network_failure) to the caller, not stall.
	caller.Send() <- &wamp.Call{Request: 3, Procedure: proc}
	select {
	case msg := <-caller.Recv():
		errMsg, ok := msg.(*wamp.Error)
		require.True(t, ok, "expected ERROR for blocked-callee call, got %T", msg)
		require.Equal(t, wamp.ErrNetworkFailure, errMsg.Error)
		require.Equal(t, wamp.ID(3), errMsg.Request)
	case <-time.After(time.Second):
		t.Fatal("dealer did not return ERROR to caller despite blocked callee")
	}

	// Drain INVOCATION 1 and 2 from callee, return YIELDs to free
	// the queue.
	for i := 1; i <= 2; i++ {
		msg, err = wamp.RecvTimeout(callee, time.Second)
		require.NoError(t, err)
		inv, ok := msg.(*wamp.Invocation)
		require.True(t, ok)
		callee.Send() <- &wamp.Yield{Request: inv.Request, Arguments: wamp.List{"ok"}}
		msg, err = wamp.RecvTimeout(caller, time.Second)
		require.NoError(t, err)
		_, ok = msg.(*wamp.Result)
		require.True(t, ok, "expected RESULT for call %d, got %T", i, msg)
	}

	// Fourth call: callee is responsive again, must succeed.
	caller.Send() <- &wamp.Call{Request: 4, Procedure: proc}
	msg, err = wamp.RecvTimeout(callee, time.Second)
	require.NoError(t, err)
	inv3, ok := msg.(*wamp.Invocation)
	require.True(t, ok, "registration should still be live; expected INVOCATION, got %T", msg)
	callee.Send() <- &wamp.Yield{Request: inv3.Request, Arguments: wamp.List{"fourth"}}
	msg, err = wamp.RecvTimeout(caller, time.Second)
	require.NoError(t, err)
	_, ok = msg.(*wamp.Result)
	require.True(t, ok, "third call should succeed after callee drained")
}

// TestCallerDisconnectMidCall verifies dealer cleanup when the caller
// disconnects after sending CALL but before YIELD arrives. The dealer's
// syncYield should observe the missing caller, log it, and clean up
// without erroring or leaking. Callee remains functional afterwards.
//
// Adjacent to #324 — covers the inverse of the slow-caller leak: a
// gone-caller, which is a more extreme version of "caller can't receive".
func TestCallerDisconnectMidCall(t *testing.T) {
	r := newTestRouter(t)

	callee := testClient(t, r)
	const proc = wamp.URI("nexus.test.midcall")
	callee.Send() <- &wamp.Register{Request: 1, Procedure: proc}
	msg, err := wamp.RecvTimeout(callee, time.Second)
	require.NoError(t, err)
	_, ok := msg.(*wamp.Registered)
	require.True(t, ok)

	caller := testClient(t, r)
	caller.Send() <- &wamp.Call{Request: 1, Procedure: proc}
	msg, err = wamp.RecvTimeout(callee, time.Second)
	require.NoError(t, err)
	inv, ok := msg.(*wamp.Invocation)
	require.True(t, ok)

	// Caller disconnects.
	caller.Close()

	// Give the realm time to process the leave (asynchronous via onLeave).
	time.Sleep(100 * time.Millisecond)

	// Callee returns YIELD for an orphaned call. Dealer must accept it
	// without error and clean up state.
	callee.Send() <- &wamp.Yield{
		Request: inv.Request, Arguments: wamp.List{"result for nobody"},
	}

	// Sanity: callee remains responsive.
	callee.Send() <- &wamp.Subscribe{Request: 100, Topic: testTopic}
	select {
	case msg := <-callee.Recv():
		_, ok := msg.(*wamp.Subscribed)
		require.True(t, ok, "expected SUBSCRIBED, got %T", msg)
	case <-time.After(time.Second):
		t.Fatal("callee not responsive after orphaned YIELD for disconnected caller")
	}
}

func TestRouterCall(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := newTestRouter(t)
		defer r.Close()
		callee := testClient(t, r)

		registerID := wamp.GlobalID()
		// Register remote procedure
		callee.Send() <- &wamp.Register{Request: registerID, Procedure: testProcedure}

		msg, err := wamp.RecvTimeout(callee, time.Second)
		require.NoError(t, err, "Timed out waiting for REGISTERED")
		registered, ok := msg.(*wamp.Registered)
		require.True(t, ok, "expected REGISTERED")
		require.Equal(t, registerID, registered.Request)
		registrationID := registered.Registration

		caller := testClient(t, r)
		callID := wamp.GlobalID()
		// Call remote procedure
		caller.Send() <- &wamp.Call{Request: callID, Procedure: testProcedure}

		msg, err = wamp.RecvTimeout(callee, time.Second)
		require.NoError(t, err, "Timed out waiting for INVOCATION")
		invocation, ok := msg.(*wamp.Invocation)
		require.True(t, ok, "expected INVOCATION")
		require.Equal(t, registrationID, invocation.Registration)
		invocationID := invocation.Request

		// Returns result of remove procedure
		callee.Send() <- &wamp.Yield{Request: invocationID}

		msg, err = wamp.RecvTimeout(caller, time.Second)
		require.NoError(t, err, "Timed out waiting for RESULT")
		result, ok := msg.(*wamp.Result)
		require.True(t, ok, "expected RESULT")
		require.Equal(t, callID, result.Request)
	})
}

func TestSessionCountMetaProcedure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := newTestRouter(t)
		defer r.Close()

		caller := testClient(t, r)

		// Call wamp.MetaProcSessionCount
		req := &wamp.Call{Request: wamp.GlobalID(), Procedure: wamp.MetaProcSessionCount}
		caller.Send() <- req
		msg, err := wamp.RecvTimeout(caller, time.Second)
		require.NoError(t, err)
		result, ok := msg.(*wamp.Result)
		require.True(t, ok, "expected RESULT")
		require.Equal(t, req.Request, result.Request)
		require.NotZero(t, len(result.Arguments), "missing expected arguemnt")
		count, ok := result.Arguments[0].(int)
		require.True(t, ok, "expected int arguemnt")
		require.Equal(t, 1, count, "wrong session count")

		// Call wamp.MetaProcSessionCount with invalid argument
		req = &wamp.Call{
			Request:   wamp.GlobalID(),
			Procedure: wamp.MetaProcSessionCount,
			Arguments: wamp.List{"should-be-a-list"},
		}
		caller.Send() <- req
		msg, err = wamp.RecvTimeout(caller, time.Second)
		require.NoError(t, err)
		errResult, ok := msg.(*wamp.Error)
		require.True(t, ok, "expected ERROR")
		require.Equal(t, req.Request, errResult.Request)

		// Call wamp.MetaProcSessionCount with non-matching filter
		filter := wamp.List{"user", "def"}
		req = &wamp.Call{
			Request:   wamp.GlobalID(),
			Procedure: wamp.MetaProcSessionCount,
			Arguments: wamp.List{filter},
		}
		caller.Send() <- req
		msg, err = wamp.RecvTimeout(caller, time.Second)
		require.NoError(t, err)
		result, ok = msg.(*wamp.Result)
		require.True(t, ok, "expected RESULT")
		require.Equal(t, req.Request, result.Request, "wrong result ID")
		require.NotZero(t, len(result.Arguments), "missing expected arguemnt")
		count = result.Arguments[0].(int)
		require.Zero(t, count, "wrong session count")

		// Call wamp.MetaProcSessionCount with matching filter
		filter = wamp.List{"trusted", "user", "def"}
		req = &wamp.Call{
			Request:   wamp.GlobalID(),
			Procedure: wamp.MetaProcSessionCount,
			Arguments: wamp.List{filter},
		}
		caller.Send() <- req
		msg, err = wamp.RecvTimeout(caller, time.Second)
		require.NoError(t, err)
		result, ok = msg.(*wamp.Result)
		require.True(t, ok, "expected RESULT")
		require.Equal(t, req.Request, result.Request)
		require.NotZero(t, len(result.Arguments), "missing expected arguemnt")
		count = result.Arguments[0].(int)
		require.Equal(t, 1, count, "wrong session count")
	})
}

func TestListSessionMetaProcedures(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := newTestRouter(t)
		defer r.Close()

		caller := testClient(t, r)
		sessID := caller.ID

		// Call wamp.MetaProcSessionList to get session list.
		req := &wamp.Call{
			Request:   wamp.GlobalID(),
			Procedure: wamp.MetaProcSessionList,
		}
		caller.Send() <- req
		msg, err := wamp.RecvTimeout(caller, time.Second)
		require.NoError(t, err)
		result, ok := msg.(*wamp.Result)
		require.True(t, ok, "expected RESULT")
		require.Equal(t, req.Request, result.Request)
		require.NotZero(t, len(result.Arguments), "missing expected arguemnt")
		ids, ok := result.Arguments[0].([]wamp.ID)
		require.True(t, ok, "wrong arg type")
		require.Equal(t, 1, len(ids), "wrong number of session IDs")
		require.Equal(t, ids[0], sessID, "wrong session ID")

		// Call wamp.MetaProcSessionList with matching filter
		filter := wamp.List{"trusted"}
		req = &wamp.Call{
			Request:   wamp.GlobalID(),
			Procedure: wamp.MetaProcSessionList,
			Arguments: wamp.List{filter},
		}
		caller.Send() <- req
		msg, err = wamp.RecvTimeout(caller, time.Second)
		require.NoError(t, err)
		result, ok = msg.(*wamp.Result)
		require.True(t, ok, "expected RESULT")
		require.Equal(t, req.Request, result.Request)
		require.NotZero(t, len(result.Arguments), "missing expected arguemnt")
		ids, ok = result.Arguments[0].([]wamp.ID)
		require.True(t, ok, "wrong arg type")
		require.Equal(t, 1, len(ids), "wrong number of session IDs")
		require.Equal(t, ids[0], sessID, "wrong session ID")
	})
}

func TestGetSessionMetaProcedures(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := newTestRouter(t)
		defer r.Close()

		caller := testClient(t, r)
		sessID := caller.ID

		// Call session meta-procedure with bad session ID
		callID := wamp.GlobalID()
		caller.Send() <- &wamp.Call{
			Request:   callID,
			Procedure: wamp.MetaProcSessionGet,
			Arguments: wamp.List{wamp.ID(123456789)},
		}
		msg, err := wamp.RecvTimeout(caller, time.Second)
		require.NoError(t, err)
		errRsp, ok := msg.(*wamp.Error)
		require.True(t, ok, "expected ERROR")
		require.Equal(t, wamp.ErrNoSuchSession, errRsp.Error, "wrong error value")

		// Call session meta-procedure to get session information.
		callID = wamp.GlobalID()
		caller.Send() <- &wamp.Call{
			Request:   callID,
			Procedure: wamp.MetaProcSessionGet,
			Arguments: wamp.List{sessID},
		}
		msg, err = wamp.RecvTimeout(caller, time.Second)
		require.NoError(t, err)
		result, ok := msg.(*wamp.Result)
		require.True(t, ok, "expected RESULT")
		require.Equal(t, callID, result.Request, "wrong result ID")
		require.NotZero(t, len(result.Arguments), "missing expected arguemnt")
		details, ok := result.Arguments[0].(wamp.Dict)
		require.True(t, ok, "expected dict type arg")
		sid, _ := wamp.AsID(details["session"])
		require.Equal(t, sessID, sid, "wrong session ID")
	})
}

func TestRegistrationMetaProcedures(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := newTestRouter(t)
		defer r.Close()
		caller := testClient(t, r)

		// ----- Test wamp.registration.list meta procedure -----
		callID := wamp.GlobalID()
		caller.Send() <- &wamp.Call{Request: callID, Procedure: wamp.MetaProcRegList}
		msg, err := wamp.RecvTimeout(caller, time.Second)
		require.NoError(t, err, "Timed out waiting for RESULT")
		result, ok := msg.(*wamp.Result)
		require.True(t, ok, "expected RESULT")
		require.Equal(t, callID, result.Request, "wrong result ID")
		require.NotZero(t, len(result.Arguments), "missing expected arguemnt")
		dict, ok := result.Arguments[0].(wamp.Dict)
		require.True(t, ok, "expected wamp.Dict")
		exactPrev, ok := dict["exact"].([]wamp.ID)
		require.True(t, ok, "expected []wamp.ID")
		prefixPrev, ok := dict["prefix"].([]wamp.ID)
		require.True(t, ok, "expected []wamp.ID")
		wildcardPrev, ok := dict["wildcard"].([]wamp.ID)
		require.True(t, ok, "expected []wamp.ID")

		callee := testClient(t, r)
		sessID := callee.ID
		// Register remote procedure
		registerID := wamp.GlobalID()
		callee.Send() <- &wamp.Register{Request: registerID, Procedure: testProcedure}

		msg, err = wamp.RecvTimeout(callee, time.Second)
		require.NoError(t, err, "Timed out waiting for REGISTERED")
		registered, ok := msg.(*wamp.Registered)
		require.True(t, ok, "expected REGISTERED")
		require.Equal(t, registerID, registered.Request, "wrong request ID")
		registrationID := registered.Registration

		// Register remote procedure
		callee.Send() <- &wamp.Register{
			Request:   wamp.GlobalID(),
			Procedure: testProcedureWC,
			Options:   wamp.Dict{"match": "wildcard"},
		}
		msg = <-callee.Recv()
		_, ok = msg.(*wamp.Registered)
		require.True(t, ok, "expected REGISTERED")

		// Call session meta-procedure to get session count.
		callID = wamp.GlobalID()
		caller.Send() <- &wamp.Call{Request: callID, Procedure: wamp.MetaProcRegList}
		msg, err = wamp.RecvTimeout(caller, time.Second)
		require.NoError(t, err, "Timed out waiting for RESULT")
		result, ok = msg.(*wamp.Result)
		require.True(t, ok, "expected RESULT")
		require.Equal(t, callID, result.Request, "wrong result ID")
		require.NotZero(t, len(result.Arguments), "missing expected arguemnt")
		dict, ok = result.Arguments[0].(wamp.Dict)
		require.True(t, ok, "expected wamp.Dict")
		exact := dict["exact"].([]wamp.ID)
		prefix := dict["prefix"].([]wamp.ID)
		wildcard := dict["wildcard"].([]wamp.ID)

		require.Equal(t, len(exactPrev)+1, len(exact), "expected additional exact match")
		require.Equal(t, len(prefixPrev), len(prefix), "prefix matches should not have changed")
		require.Equal(t, len(wildcardPrev)+1, len(wildcard), "expected additional wildcard match")

		var found bool
		if slices.Contains(exact, registrationID) {
			found = true
		}
		require.True(t, found, "missing expected registration ID")

		// ----- Test wamp.registration.lookup meta procedure -----
		callID = wamp.GlobalID()
		caller.Send() <- &wamp.Call{
			Request:   callID,
			Procedure: wamp.MetaProcRegLookup,
			Arguments: wamp.List{testProcedure},
		}
		msg, err = wamp.RecvTimeout(caller, time.Second)
		require.NoError(t, err, "Timed out waiting for RESULT")
		result, ok = msg.(*wamp.Result)
		require.True(t, ok, "expected RESULT")
		require.Equal(t, callID, result.Request, "wrong result ID")
		require.NotZero(t, len(result.Arguments), "missing expected arguemnt")
		regID, ok := result.Arguments[0].(wamp.ID)
		require.True(t, ok, "expected wamp.ID")
		require.Equal(t, registrationID, regID, "received wrong registration ID")

		// ----- Test wamp.registration.match meta procedure -----
		callID = wamp.GlobalID()
		caller.Send() <- &wamp.Call{
			Request:   callID,
			Procedure: wamp.MetaProcRegMatch,
			Arguments: wamp.List{testProcedure},
		}
		msg, err = wamp.RecvTimeout(caller, time.Second)
		require.NoError(t, err, "Timed out waiting for RESULT")
		result, ok = msg.(*wamp.Result)
		require.True(t, ok, "expected RESULT")
		require.Equal(t, callID, result.Request, "wrong result ID")
		require.NotZero(t, len(result.Arguments), "missing expected arguemnt")
		regID, ok = wamp.AsID(result.Arguments[0])
		require.True(t, ok, "expected wamp.ID")
		require.Equal(t, registrationID, regID, "received wrong registration ID")

		// ----- Test wamp.registration.get meta procedure -----
		callID = wamp.GlobalID()
		caller.Send() <- &wamp.Call{
			Request:   callID,
			Procedure: wamp.MetaProcRegGet,
			Arguments: wamp.List{registrationID},
		}
		msg, err = wamp.RecvTimeout(caller, time.Second)
		require.NoError(t, err, "Timed out waiting for RESULT")
		result, ok = msg.(*wamp.Result)
		require.True(t, ok, "expected RESULT")
		require.Equal(t, callID, result.Request, "wrong result ID")
		require.NotZero(t, len(result.Arguments), "missing expected arguemnt")
		dict, ok = result.Arguments[0].(wamp.Dict)
		require.True(t, ok, "expected wamp.Dict")
		regID, _ = wamp.AsID(dict["id"])
		require.Equal(t, registrationID, regID, "received wrong registration")
		uri, _ := wamp.AsURI(dict["uri"])
		require.Equal(t, testProcedure, uri, "registration has wrong uri")

		// ----- Test wamp.registration.list_callees meta procedure -----
		callID = wamp.GlobalID()
		caller.Send() <- &wamp.Call{
			Request:   callID,
			Procedure: wamp.MetaProcRegListCallees,
			Arguments: wamp.List{registrationID},
		}
		msg, err = wamp.RecvTimeout(caller, time.Second)
		require.NoError(t, err, "Timed out waiting for RESULT")
		result, ok = msg.(*wamp.Result)
		require.True(t, ok, "expected RESULT")
		require.Equal(t, callID, result.Request, "wrong result ID")
		require.NotZero(t, len(result.Arguments), "missing expected arguemnt")
		idList, ok := result.Arguments[0].([]wamp.ID)
		require.True(t, ok, "Expected []wamp.ID")
		require.Equal(t, 1, len(idList), "Expected 1 callee in list")
		require.Equal(t, sessID, idList[0], "Wrong callee session ID")

		// ----- Test wamp.registration.count_callees meta procedure -----
		callID = wamp.GlobalID()
		caller.Send() <- &wamp.Call{
			Request:   callID,
			Procedure: wamp.MetaProcRegCountCallees,
			Arguments: wamp.List{registrationID},
		}
		msg, err = wamp.RecvTimeout(caller, time.Second)
		require.NoError(t, err, "Timed out waiting for RESULT")
		result, ok = msg.(*wamp.Result)
		require.True(t, ok, "expected RESULT")
		require.Equal(t, callID, result.Request, "wrong result ID")
		require.NotZero(t, len(result.Arguments), "missing expected arguemnt")
		count, ok := wamp.AsInt64(result.Arguments[0])
		require.True(t, ok, "Argument is not an int")
		require.Equal(t, int64(1), count, "Wring number of callees")
	})
}

func TestSubscriptionMetaProcedures(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := newTestRouter(t)
		defer r.Close()
		caller := testClient(t, r)

		// ----- Test wamp.subscription.list meta procedure -----
		callID := wamp.GlobalID()
		caller.Send() <- &wamp.Call{Request: callID, Procedure: wamp.MetaProcSubList}
		msg, err := wamp.RecvTimeout(caller, time.Second)
		require.NoError(t, err, "Timed out waiting for RESULT")
		result, ok := msg.(*wamp.Result)
		require.True(t, ok, "expected RESULT")
		require.Equal(t, callID, result.Request, "wrong result ID")
		require.NotZero(t, len(result.Arguments), "missing expected arguemnt")
		dict, ok := result.Arguments[0].(wamp.Dict)
		require.True(t, ok, "expected wamp.Dict")
		exactPrev, ok := dict["exact"].([]wamp.ID)
		require.True(t, ok, "expected []wamp.ID")
		prefixPrev, ok := dict["prefix"].([]wamp.ID)
		require.True(t, ok, "expected []wamp.ID")
		wildcardPrev, ok := dict["wildcard"].([]wamp.ID)
		require.True(t, ok, "expected []wamp.ID")

		subscriber := testClient(t, r)
		sessID := subscriber.ID
		// Subscribe to topic
		reqID := wamp.GlobalID()
		subscriber.Send() <- &wamp.Subscribe{Request: reqID, Topic: testTopic}
		msg, err = wamp.RecvTimeout(subscriber, time.Second)
		require.NoError(t, err, "Timed out waiting for SUBSCRIBED")
		subscribed, ok := msg.(*wamp.Subscribed)
		require.True(t, ok, "expected SUBSCRIBED")
		require.Equal(t, reqID, subscribed.Request, "wrong request ID")
		subscriptionID := subscribed.Subscription

		// Subscriber to wildcard topic
		subscriber.Send() <- &wamp.Subscribe{
			Request: wamp.GlobalID(),
			Topic:   testTopicWC,
			Options: wamp.Dict{"match": "wildcard"},
		}
		msg = <-subscriber.Recv()
		subscribed, ok = msg.(*wamp.Subscribed)
		require.True(t, ok, "expected SUBSCRIBED")
		wcSubscriptionID := subscribed.Subscription

		// Call subscription meta-procedure to get subscriptions.
		callID = wamp.GlobalID()
		caller.Send() <- &wamp.Call{Request: callID, Procedure: wamp.MetaProcSubList}
		msg, err = wamp.RecvTimeout(caller, time.Second)
		require.NoError(t, err, "Timed out waiting for RESULT")
		result, ok = msg.(*wamp.Result)
		require.True(t, ok, "expected RESULT")
		require.Equal(t, callID, result.Request, "wrong result ID")
		require.NotZero(t, len(result.Arguments), "missing expected arguemnt")
		dict, ok = result.Arguments[0].(wamp.Dict)
		require.True(t, ok, "expected wamp.Dict")
		exact := dict["exact"].([]wamp.ID)
		prefix := dict["prefix"].([]wamp.ID)
		wildcard := dict["wildcard"].([]wamp.ID)

		require.Equal(t, len(exactPrev)+1, len(exact), "expected additional exact match")
		require.Equal(t, len(prefixPrev), len(prefix), "prefix matches should not have changed")
		require.Equal(t, len(wildcardPrev)+1, len(wildcard), "expected additional wildcard match")

		var found bool
		if slices.Contains(exact, subscriptionID) {
			found = true
		}
		require.True(t, found, "missing expected subscription ID")

		// ----- Test wamp.subscription.lookup meta procedure -----
		callID = wamp.GlobalID()
		caller.Send() <- &wamp.Call{
			Request:   callID,
			Procedure: wamp.MetaProcSubLookup,
			Arguments: wamp.List{testTopic},
		}
		msg, err = wamp.RecvTimeout(caller, time.Second)
		require.NoError(t, err, "Timed out waiting for RESULT")
		result, ok = msg.(*wamp.Result)
		require.True(t, ok, "expected RESULT")
		require.Equal(t, callID, result.Request, "wrong result ID")
		require.NotZero(t, len(result.Arguments), "missing expected arguemnt")
		subID, ok := result.Arguments[0].(wamp.ID)
		require.True(t, ok, "expected wamp.ID")
		require.Equal(t, subscriptionID, subID, "received wrong subscription ID")

		// ----- Test wamp.subscription.match meta procedure -----
		callID = wamp.GlobalID()
		caller.Send() <- &wamp.Call{
			Request:   callID,
			Procedure: wamp.MetaProcSubMatch,
			Arguments: wamp.List{testTopic},
		}
		msg, err = wamp.RecvTimeout(caller, time.Second)
		require.NoError(t, err, "Timed out waiting for RESULT")
		result, ok = msg.(*wamp.Result)
		require.True(t, ok, "expected RESULT")
		require.Equal(t, callID, result.Request, "wrong result ID")
		require.NotZero(t, len(result.Arguments), "missing expected arguemnt")
		subIDs, ok := wamp.AsList(result.Arguments[0])
		require.True(t, ok, "expected wamp.List")
		require.Equal(t, 2, len(subIDs), "expected 2 subscriptions for wamp.subscription.match")
		require.Contains(t, subIDs, subscriptionID, "did not match subscription ID")
		require.Contains(t, subIDs, wcSubscriptionID, "did not match wildcard subscription ID")

		// ----- Test wamp.subscription.get meta procedure -----
		callID = wamp.GlobalID()
		caller.Send() <- &wamp.Call{
			Request:   callID,
			Procedure: wamp.MetaProcSubGet,
			Arguments: wamp.List{subscriptionID},
		}
		msg, err = wamp.RecvTimeout(caller, time.Second)
		require.NoError(t, err, "Timed out waiting for RESULT")
		result, ok = msg.(*wamp.Result)
		require.True(t, ok, "expected RESULT")
		require.Equal(t, callID, result.Request, "wrong result ID")
		require.NotZero(t, len(result.Arguments), "missing expected arguemnt")
		dict, ok = result.Arguments[0].(wamp.Dict)
		require.True(t, ok, "expected wamp.Dict")
		subID, _ = wamp.AsID(dict["id"])
		require.Equal(t, subscriptionID, subID, "received wrong subscription")
		uri, _ := wamp.AsURI(dict["uri"])
		require.Equal(t, testTopic, uri, "subscription has wrong uri")

		// ----- Test wamp.subscription.list_subscribers meta procedure -----
		callID = wamp.GlobalID()
		caller.Send() <- &wamp.Call{
			Request:   callID,
			Procedure: wamp.MetaProcSubListSubscribers,
			Arguments: wamp.List{subscriptionID},
		}
		msg, err = wamp.RecvTimeout(caller, time.Second)
		require.NoError(t, err, "Timed out waiting for RESULT")
		result, ok = msg.(*wamp.Result)
		require.True(t, ok, "expected RESULT")
		require.Equal(t, callID, result.Request, "wrong result ID")
		require.NotZero(t, len(result.Arguments), "missing expected arguemnt")
		idList, ok := result.Arguments[0].([]wamp.ID)
		require.True(t, ok, "Expected []wamp.ID")
		require.Equal(t, 1, len(idList), "Expected 1 subscriber in list")
		require.Equal(t, sessID, idList[0], "Wrong subscriber session ID")

		// ----- Test wamp.subscription.count_subscribers meta procedure -----
		// The spec name, and the misspelled legacy alias.
		require.Equal(t, wamp.URI("wamp.subscription.count_subscribers"), wamp.MetaProcSubCountSubscribers)
		for _, proc := range []wamp.URI{wamp.MetaProcSubCountSubscribers, wamp.MetaProcSubCountSuscribers} { //nolint:staticcheck // the deprecated alias must keep working
			callID = wamp.GlobalID()
			caller.Send() <- &wamp.Call{
				Request:   callID,
				Procedure: proc,
				Arguments: wamp.List{subscriptionID},
			}
			msg, err = wamp.RecvTimeout(caller, time.Second)
			require.NoError(t, err, "Timed out waiting for RESULT")
			result, ok = msg.(*wamp.Result)
			require.True(t, ok, "expected RESULT from %s, got %v", proc, msg)
			require.Equal(t, callID, result.Request, "wrong result ID")
			require.NotZero(t, result.Arguments, "missing expected arguemnt")
			count, ok := wamp.AsInt64(result.Arguments[0])
			require.True(t, ok, "Argument is not an int")
			require.Equal(t, int64(1), count, "Wring number of subscribers")
		}
	})
}

func TestDynamicRealmChange(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {

		dr := newTestRouter(t)
		err := dr.AddRealm(&RealmConfig{
			URI:           testRealm2,
			StrictURI:     false,
			AnonymousAuth: true,
			AllowDisclose: false,
		})
		require.NoError(t, err)

		cli := testClientInRealm(t, dr, testRealm2)
		cli.Send() <- &wamp.Goodbye{}
		msg, err := wamp.RecvTimeout(cli, time.Second)
		require.NoError(t, err, "no goodbye message after sending goodbye")
		_, ok := msg.(*wamp.Goodbye)
		require.True(t, ok, "expected GOODBYE")

		cli = testClientInRealm(t, dr, testRealm2)
		sync := make(chan wamp.Message)
		go func() {
			sync <- <-cli.Recv()
		}()

		dr.RemoveRealm(testRealm2)

		select {
		case <-time.After(time.Second):
			require.FailNow(t, "expected client to be booted when removing realm")
		case msg := <-sync:
			_, ok := msg.(*wamp.Goodbye)
			require.True(t, ok, "expected GOODBYE")
		}
	})
}
