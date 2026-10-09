package client

import (
	"context"
	"io"
	"log"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ironflock/nexus/v3/transport"
	"github.com/ironflock/nexus/v3/wamp"
)

// newWelcomedClient returns a client joined to a router stand-in that answers
// the HELLO and nothing else.
func newWelcomedClient(t *testing.T) *Client {
	t.Helper()
	c, _ := newWelcomedClientPeer(t)
	return c
}

// newWelcomedClientPeer is newWelcomedClient that also returns the router end
// of the connection.
func newWelcomedClientPeer(t *testing.T) (*Client, wamp.Peer) {
	t.Helper()
	clientSide, routerSide := transport.LinkedPeers()
	go func() {
		<-routerSide.Recv() // HELLO
		routerSide.Send() <- &wamp.Welcome{
			ID: 1,
			Details: wamp.Dict{
				"roles": wamp.Dict{"broker": wamp.Dict{}, "dealer": wamp.Dict{}},
			},
		}
	}()
	c, err := NewClient(clientSide, Config{Realm: "nexus.test", Logger: log.New(io.Discard, "", 0)})
	require.NoError(t, err)
	t.Cleanup(func() {
		// Answer the GOODBYE of Close, as a router does, so that Close
		// returns at once.
		go func() {
			for msg := range routerSide.Recv() {
				if _, ok := msg.(*wamp.Goodbye); ok {
					routerSide.Send() <- &wamp.Goodbye{Reason: wamp.CloseGoodbyeAndOut, Details: wamp.Dict{}}
				}
			}
		}()
		_ = c.Close()
		routerSide.Close()
	})
	return c, routerSide
}

// runSignalReply runs on run(), so it must not block on a waiter that has
// stopped waiting. A waiter gives up at its response timeout, possibly just
// after run() found its entry, and then nothing takes the reply any more. That
// used to park run() for good: the client read nothing more, Done never
// closed, and Close hung.
func TestRunSignalReplyReturnsWhenWaiterHasLeft(t *testing.T) {
	const id = wamp.ID(1)
	for name, replies := range map[string][]wamp.Message{
		"single reply": {
			&wamp.Subscribed{Request: id, Subscription: 2},
		},
		"progressive results": {
			&wamp.Result{Request: id, Details: wamp.Dict{wamp.OptProgress: true}},
			&wamp.Result{Request: id, Details: wamp.Dict{}},
		},
	} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				c := newWelcomedClient(t)
				c.expectReply(id)
				signalled := make(chan struct{})
				go func() {
					defer close(signalled)
					for _, msg := range replies {
						c.runSignalReply(msg, id)
					}
				}()
				synctest.Wait()
				c.dropReply(id) // the waiter gives up, as at its response timeout
				synctest.Wait()
				select {
				case <-signalled:
				default:
					t.Fatal("runSignalReply blocked on a waiter that has left")
				}
			})
		})
	}
}

// A SUBSCRIBED handed over as the waiter's timer fires must not leave a
// handler installed behind a Subscribe that reports a timeout. run() installs
// the handler in the step that hands over the reply, and the waiter returns a
// reply it was handed even when its timer fired too.
func TestReplyHandedOverAtTimeoutIsReturned(t *testing.T) {
	c := newWelcomedClient(t)
	c.responseTimeout = time.Nanosecond // the timer and the reply race
	for i := range 200 {
		id := wamp.ID(i + 1)
		installed := false
		c.expectReplyInstall(id, func(wamp.Message) { installed = true })
		c.runSignalReply(&wamp.Subscribed{Request: id, Subscription: 2}, id)
		msg, err := c.waitForReply(id)
		require.NoError(t, err)
		require.IsType(t, &wamp.Subscribed{}, msg)
		require.True(t, installed)
	}
}

// Once Close has given up waiting for run() and canceled the client (see
// awaitRunExit), run() handles no further message, and runHandleInvocation
// starts no handler, which Close's wait for the active handlers could miss.
func TestCanceledClientStartsNoHandler(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c, routerSide := newWelcomedClientPeer(t)
		var handled atomic.Int32
		c.sess.Lock()
		c.eventHandlers[1] = func(*wamp.Event) { handled.Add(1) }
		c.invHandlers[2] = func(context.Context, *wamp.Invocation) InvokeResult {
			handled.Add(1)
			return InvokeResult{}
		}
		c.cancel() // as awaitRunExit does
		c.sess.Unlock()

		for i := range 20 {
			routerSide.Send() <- &wamp.Event{Subscription: 1, Publication: wamp.ID(i + 1), Details: wamp.Dict{}}
			c.runHandleInvocation(&wamp.Invocation{Request: wamp.ID(i + 1), Registration: 2, Details: wamp.Dict{}})
		}
		synctest.Wait()
		require.Zero(t, handled.Load(), "handlers ran on a canceled client")
	})
}
