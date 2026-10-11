package client_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ironflock/nexus/v3/client"
	"github.com/ironflock/nexus/v3/transport"
	"github.com/ironflock/nexus/v3/wamp"
)

// scriptedRouter is the router end of a LinkedPeers pair, driven message by
// message by the test. It can put a reply and the message right behind it on
// the wire back to back, which a real router does only now and then.
type scriptedRouter struct {
	t    *testing.T
	peer wamp.Peer
}

// connectScripted joins a client to a scripted router.
func connectScripted(t *testing.T, cfg *client.Config) (*client.Client, *scriptedRouter) {
	t.Helper()
	clientSide, routerSide := transport.LinkedPeers()
	r := &scriptedRouter{t: t, peer: routerSide}

	type joined struct {
		c   *client.Client
		err error
	}
	res := make(chan joined, 1)
	go func() {
		c, err := client.NewClient(clientSide, *cfg)
		res <- joined{c, err}
	}()
	expectMsg[*wamp.Hello](r)
	r.send(&wamp.Welcome{
		ID: 1,
		Details: wamp.Dict{
			"roles": wamp.Dict{"broker": wamp.Dict{}, "dealer": wamp.Dict{}},
		},
	})
	j := <-res
	require.NoError(t, j.err)
	t.Cleanup(func() {
		answerGoodbye(routerSide)
		_ = j.c.Close()
		routerSide.Close()
	})
	return j.c, r
}

// answerGoodbye answers the client's GOODBYE, as a router does, so that Close
// returns at once. It reads until the connection ends.
func answerGoodbye(routerSide wamp.Peer) {
	go func() {
		for msg := range routerSide.Recv() {
			if _, ok := msg.(*wamp.Goodbye); ok {
				routerSide.Send() <- &wamp.Goodbye{Reason: wamp.CloseGoodbyeAndOut, Details: wamp.Dict{}}
			}
		}
	}()
}

// send writes msgs to the client in order.
func (r *scriptedRouter) send(msgs ...wamp.Message) {
	for _, msg := range msgs {
		r.peer.Send() <- msg
	}
}

// recv returns the next message the client sent.
func (r *scriptedRouter) recv() wamp.Message {
	r.t.Helper()
	select {
	case msg, ok := <-r.peer.Recv():
		require.True(r.t, ok, "client closed the connection")
		return msg
	case <-time.After(5 * time.Second):
		require.FailNow(r.t, "timed out waiting for a message from the client")
	}
	return nil
}

// expectMsg returns the next message the client sent, which must be a T.
func expectMsg[T wamp.Message](r *scriptedRouter) T {
	r.t.Helper()
	msg := r.recv()
	m, ok := msg.(T)
	require.Truef(r.t, ok, "client sent %s, expected %T", msg.MessageType(), m)
	return m
}

// lineLog is a client logger that hands every line to a channel, so a test can
// tell "the client dropped this message" (it logs that) from "not yet".
type lineLog struct{ lines chan string }

func newLineLog() *lineLog { return &lineLog{lines: make(chan string, 1024)} }

func (l *lineLog) put(s string) {
	select {
	case l.lines <- strings.TrimSpace(s):
	default: // never block the client on its logger
	}
}

func (l *lineLog) Print(v ...any)                 { l.put(fmt.Sprint(v...)) }
func (l *lineLog) Println(v ...any)               { l.put(fmt.Sprintln(v...)) }
func (l *lineLog) Printf(format string, v ...any) { l.put(fmt.Sprintf(format, v...)) }

// expectLogged waits until the client logs a line containing substr.
func (l *lineLog) expectLogged(t *testing.T, substr string) {
	t.Helper()
	for {
		select {
		case line := <-l.lines:
			if strings.Contains(line, substr) {
				return
			}
		case <-time.After(5 * time.Second):
			require.FailNow(t, "client did not log the expected line", "want a line containing %q", substr)
		}
	}
}

func withLogger(l *lineLog) clientConfigMutator {
	return func(cfg *client.Config) { cfg.Logger = l }
}

func withResponseTimeout(d time.Duration) clientConfigMutator {
	return func(cfg *client.Config) { cfg.ResponseTimeout = d }
}

// An EVENT the router sends right behind SUBSCRIBED (a retained event, or the
// next event of a busy topic) must reach the handler being subscribed. The
// handler used to be installed by the Subscribe caller after it woke up, while
// run() had already moved on to the EVENT and dropped it for want of one.
func TestEventRightBehindSubscribedIsDelivered(t *testing.T) {
	logs := newLineLog()
	c, r := connectScripted(t, newTestClientConfig(testRealm, withLogger(logs)))

	const rounds = 200
	var dropped int
	var firstDrop string
	for i := range rounds {
		topic := fmt.Sprintf("test.first.event.%d", i)
		subID := wamp.ID(1000 + i)
		events := make(chan *wamp.Event, 1)
		errc := make(chan error, 1)
		go func() {
			errc <- c.Subscribe(topic, func(ev *wamp.Event) { events <- ev }, nil)
		}()
		sub := expectMsg[*wamp.Subscribe](r)
		r.send(
			&wamp.Subscribed{Request: sub.Request, Subscription: subID},
			&wamp.Event{Subscription: subID, Publication: wamp.ID(i + 1), Details: wamp.Dict{}},
		)
		require.NoError(t, <-errc)

		// The client either runs the handler or logs that it has none.
		select {
		case ev := <-events:
			require.Equal(t, subID, ev.Subscription)
		case line := <-logs.lines:
			if dropped == 0 {
				firstDrop = line
			}
			dropped++
		case <-time.After(5 * time.Second):
			require.FailNow(t, "event neither delivered nor dropped", "round %d", i)
		}
	}
	require.Zero(t, dropped, "%d of %d events right behind SUBSCRIBED were dropped; first: %s",
		dropped, rounds, firstDrop)
}

// An INVOCATION the router sends right behind REGISTERED (a caller polling the
// procedure until it exists, say) must reach the handler being registered,
// not be refused with wamp.error.invalid_argument "client has no handler for
// registration".
func TestInvocationRightBehindRegisteredIsHandled(t *testing.T) {
	c, r := connectScripted(t, newTestClientConfig(testRealm, withLogger(newLineLog())))

	const rounds = 200
	var refused int
	var firstRefusal string
	for i := range rounds {
		procedure := fmt.Sprintf("test.first.call.%d", i)
		regID := wamp.ID(1000 + i)
		errc := make(chan error, 1)
		go func() {
			errc <- c.Register(procedure, func(context.Context, *wamp.Invocation) client.InvokeResult {
				return client.InvokeResult{Args: wamp.List{"ok"}}
			}, nil)
		}()
		reg := expectMsg[*wamp.Register](r)
		r.send(
			&wamp.Registered{Request: reg.Request, Registration: regID},
			&wamp.Invocation{Request: wamp.ID(i + 1), Registration: regID, Details: wamp.Dict{}},
		)
		require.NoError(t, <-errc)

		switch msg := r.recv().(type) {
		case *wamp.Yield:
			require.Equal(t, wamp.ID(i+1), msg.Request)
		case *wamp.Error:
			if refused == 0 {
				firstRefusal = fmt.Sprint(msg.Error, " ", msg.Arguments)
			}
			refused++
		default:
			require.FailNow(t, "unexpected reply to INVOCATION", "round %d: %s", i, msg.MessageType())
		}
	}
	require.Zero(t, refused, "%d of %d invocations right behind REGISTERED were refused; first: %s",
		refused, rounds, firstRefusal)
}

// A SUBSCRIBE or REGISTER that fails installs no handler: neither when the
// router refuses it, nor when SUBSCRIBED or REGISTERED arrives after the
// response timeout, by which time the caller has been told that it failed.
func TestNoHandlerInstalledForFailedRequest(t *testing.T) {
	logs := newLineLog()
	c, r := connectScripted(t, newTestClientConfig(testRealm, withLogger(logs),
		withResponseTimeout(50*time.Millisecond)))
	onEvent := func(*wamp.Event) { t.Error("event handler of a failed Subscribe called") }
	onInvocation := func(context.Context, *wamp.Invocation) client.InvokeResult {
		t.Error("invocation handler of a failed Register called")
		return client.InvokeResult{Err: wamp.ErrInvalidArgument}
	}
	errc := make(chan error, 1)

	t.Run("ERROR reply", func(t *testing.T) {
		go func() { errc <- c.Subscribe("test.refused", onEvent, nil) }()
		sub := expectMsg[*wamp.Subscribe](r)
		r.send(
			&wamp.Error{Type: wamp.SUBSCRIBE, Request: sub.Request, Details: wamp.Dict{}, Error: wamp.ErrNotAuthorized},
			&wamp.Event{Subscription: 7, Publication: 1, Details: wamp.Dict{}},
		)
		require.ErrorContains(t, <-errc, string(wamp.ErrNotAuthorized))
		logs.expectLogged(t, "No handler registered for subscription: 7")

		go func() { errc <- c.Register("test.refused", onInvocation, nil) }()
		reg := expectMsg[*wamp.Register](r)
		r.send(
			&wamp.Error{Type: wamp.REGISTER, Request: reg.Request, Details: wamp.Dict{}, Error: wamp.ErrNotAuthorized},
			&wamp.Invocation{Request: 1, Registration: 8, Details: wamp.Dict{}},
		)
		require.ErrorContains(t, <-errc, string(wamp.ErrNotAuthorized))
		require.Equal(t, wamp.ErrInvalidArgument, expectMsg[*wamp.Error](r).Error)
	})

	t.Run("reply after the timeout", func(t *testing.T) {
		go func() { errc <- c.Subscribe("test.late", onEvent, nil) }()
		sub := expectMsg[*wamp.Subscribe](r)
		require.ErrorIs(t, <-errc, client.ErrReplyTimeout)
		r.send(
			&wamp.Subscribed{Request: sub.Request, Subscription: 9},
			&wamp.Event{Subscription: 9, Publication: 2, Details: wamp.Dict{}},
		)
		logs.expectLogged(t, "that client is no longer waiting for")
		logs.expectLogged(t, "No handler registered for subscription: 9")
		_, ok := c.SubscriptionID("test.late")
		require.False(t, ok, "timed-out Subscribe left a subscription behind")

		go func() { errc <- c.Register("test.late", onInvocation, nil) }()
		reg := expectMsg[*wamp.Register](r)
		require.ErrorIs(t, <-errc, client.ErrReplyTimeout)
		r.send(
			&wamp.Registered{Request: reg.Request, Registration: 10},
			&wamp.Invocation{Request: 2, Registration: 10, Details: wamp.Dict{}},
		)
		require.Equal(t, wamp.ErrInvalidArgument, expectMsg[*wamp.Error](r).Error)
		_, ok = c.RegistrationID("test.late")
		require.False(t, ok, "timed-out Register left a registration behind")
	})
}

// Close must return even when run() cannot finish the message in hand: here
// it is handing a progressive RESULT to a Call whose progress handler does not
// return. Close used to wait for run() without a bound once the router did not
// answer its GOODBYE.
func TestCloseReturnsWhileRunLoopIsBlocked(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c, r := connectScripted(t, newTestClientConfig(testRealm, withLogger(newLineLog())))
		release := make(chan struct{})
		unblock := sync.OnceFunc(func() { close(release) })
		t.Cleanup(unblock)

		callErr := make(chan error, 1)
		go func() {
			_, err := c.Call(context.Background(), "test.progress", nil, nil, nil,
				func(*wamp.Result) { <-release })
			callErr <- err
		}()
		call := expectMsg[*wamp.Call](r)
		for range 4 {
			r.send(&wamp.Result{Request: call.Request, Details: wamp.Dict{wamp.OptProgress: true}})
		}
		synctest.Wait() // run() is stuck handing over a progressive result

		closed := make(chan error, 1)
		go func() { closed <- c.Close() }()
		expectMsg[*wamp.Goodbye](r) // and never answered
		select {
		case err := <-closed:
			require.NoError(t, err)
		case <-time.After(time.Minute):
			require.FailNow(t, "Close did not return while run() was blocked")
		}
		select {
		case <-c.Done():
		default:
			require.FailNow(t, "Done not closed after Close returned")
		}

		unblock()
		require.ErrorIs(t, <-callErr, client.ErrNotConn)
	})
}
