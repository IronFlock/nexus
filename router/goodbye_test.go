package router //nolint:testpackage

import (
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"

	"github.com/ironflock/nexus/v3/transport/serialize"
	"github.com/ironflock/nexus/v3/wamp"
)

// rawWSClient is a websocket client that speaks JSON WAMP frames directly
// and keeps reading after GOODBYE, like autobahn does.
type rawWSClient struct {
	t    *testing.T
	conn *websocket.Conn
	ser  serialize.JSONSerializer
}

func dialRawWS(t *testing.T, r Router) *rawWSClient {
	t.Helper()
	srv := httptest.NewServer(NewWebsocketServer(r))
	t.Cleanup(srv.Close)
	dialer := websocket.Dialer{Subprotocols: []string{jsonWebsocketProtocol}}
	conn, _, err := dialer.Dial("ws"+strings.TrimPrefix(srv.URL, "http"), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	c := &rawWSClient{t: t, conn: conn}
	c.send(&wamp.Hello{Realm: testRealm, Details: clientRoles})
	c.expect(wamp.WELCOME)
	return c
}

func (c *rawWSClient) send(msg wamp.Message) {
	c.t.Helper()
	b, err := c.ser.Serialize(msg)
	require.NoError(c.t, err)
	require.NoError(c.t, c.conn.WriteMessage(websocket.TextMessage, b))
}

// recv returns the next message, or nil once the connection is closed or
// nothing arrives within the timeout.
func (c *rawWSClient) recv(timeout time.Duration) wamp.Message {
	c.t.Helper()
	require.NoError(c.t, c.conn.SetReadDeadline(time.Now().Add(timeout)))
	_, b, err := c.conn.ReadMessage()
	if err != nil {
		return nil
	}
	msg, err := c.ser.Deserialize(b)
	require.NoError(c.t, err)
	return msg
}

// expect skips messages until one of type want arrives.
func (c *rawWSClient) expect(want wamp.MessageType) wamp.Message {
	c.t.Helper()
	for {
		msg := c.recv(time.Second)
		require.NotNilf(c.t, msg, "no %s received", want)
		if msg.MessageType() == want {
			return msg
		}
	}
}

// requireSilentAfterGoodbye reads until the connection closes or goes quiet
// for quiet, and fails on anything received after the router's GOODBYE.
func (c *rawWSClient) requireSilentAfterGoodbye(quiet time.Duration) {
	c.t.Helper()
	c.expect(wamp.GOODBYE)
	for msg := c.recv(quiet); msg != nil; msg = c.recv(quiet) {
		c.t.Fatalf("router sent %s after GOODBYE: %+v", msg.MessageType(), msg)
	}
}

// publishLoop publishes to topic as fast as the realm accepts until the test
// ends. It publishes through the realm's meta session, which neither a
// client GOODBYE nor a drain kicks.
func publishLoop(t *testing.T, r Router, topic wamp.URI) {
	t.Helper()
	rl := r.(*router).realms[testRealm]
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			case rl.metaPeer.Send() <- &wamp.Publish{
				Request: wamp.GlobalID(),
				Options: wamp.Dict{},
				Topic:   topic,
			}:
			}
		}
	}()
	t.Cleanup(func() {
		close(stop)
		<-done
	})
}

// A client that leaves must receive nothing after the router's GOODBYE:
// neither the wamp.registration.on_delete event for its own registration
// (the case reported in gammazero/nexus#347) nor events routed to it before
// the broker dropped its subscriptions. A steady event stream keeps that
// window busy, and the scenario runs several times because it is a race.
func TestNoEventAfterClientGoodbye(t *testing.T) {
	r := newTestRouter(t)
	publishLoop(t, r, "test.goodbye.topic")
	for i := 0; i < 50; i++ {
		c := dialRawWS(t, r)
		c.send(&wamp.Subscribe{Request: 1, Options: wamp.Dict{}, Topic: wamp.MetaEventRegOnDelete})
		c.expect(wamp.SUBSCRIBED)
		c.send(&wamp.Register{Request: 2, Options: wamp.Dict{}, Procedure: "test.goodbye.proc"})
		c.expect(wamp.REGISTERED)
		c.send(&wamp.Subscribe{Request: 3, Options: wamp.Dict{}, Topic: "test.goodbye.topic"})
		c.expect(wamp.SUBSCRIBED)
		c.expect(wamp.EVENT)

		c.send(&wamp.Goodbye{Reason: wamp.CloseNormal, Details: wamp.Dict{}})
		c.requireSilentAfterGoodbye(200 * time.Millisecond)
	}
}

// A client that keeps its connection open after the GOODBYE of a router
// drain must not receive the events still published in the realm. The
// drained session stays subscribed until the realm closes, so without the
// transport gate every one of them would be delivered.
func TestNoEventAfterDrainGoodbye(t *testing.T) {
	r := newTestRouter(t)
	publishLoop(t, r, "test.goodbye.topic")
	c := dialRawWS(t, r)
	c.send(&wamp.Subscribe{Request: 1, Options: wamp.Dict{}, Topic: "test.goodbye.topic"})
	c.expect(wamp.SUBSCRIBED)
	c.expect(wamp.EVENT)

	r.Drain()
	gb := c.expect(wamp.GOODBYE).(*wamp.Goodbye)
	require.Equal(t, wamp.ErrSystemShutdown, gb.Reason)
	for msg := c.recv(200 * time.Millisecond); msg != nil; msg = c.recv(200 * time.Millisecond) {
		t.Fatalf("router sent %s after drain GOODBYE: %+v", msg.MessageType(), msg)
	}
}
