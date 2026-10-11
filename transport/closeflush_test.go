package transport_test

import (
	"io"
	"log"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"

	"github.com/ironflock/nexus/v3/transport"
	"github.com/ironflock/nexus/v3/transport/serialize"
	"github.com/ironflock/nexus/v3/wamp"
)

// heldWriteConn blocks its first WriteMessage until release is closed, so
// frames queue up behind it, and records every frame and write deadline.
type heldWriteConn struct {
	frameRecordingConn
	release   chan struct{}
	writing   chan struct{}
	firstOnce sync.Once

	dmu       sync.Mutex
	deadlines []time.Time
}

func newHeldWriteConn() *heldWriteConn {
	return &heldWriteConn{
		frameRecordingConn: frameRecordingConn{closed: make(chan struct{})},
		release:            make(chan struct{}),
		writing:            make(chan struct{}),
	}
}

func (m *heldWriteConn) WriteMessage(messageType int, data []byte) error {
	m.firstOnce.Do(func() {
		close(m.writing)
		<-m.release
	})
	return m.frameRecordingConn.WriteMessage(messageType, data)
}

func (m *heldWriteConn) SetWriteDeadline(t time.Time) error {
	m.dmu.Lock()
	defer m.dmu.Unlock()
	m.deadlines = append(m.deadlines, t)
	return nil
}

func (m *heldWriteConn) recordedDeadlines() []time.Time {
	m.dmu.Lock()
	defer m.dmu.Unlock()
	return append([]time.Time(nil), m.deadlines...)
}

// closeWithQueue writes a first frame and holds it on the wire, queues
// queued behind it, calls Close, then lets the first write finish. It
// returns once Close has returned.
func closeWithQueue(t *testing.T, conn *heldWriteConn, keepAlive time.Duration, queued ...wamp.Message) {
	t.Helper()
	peer := transport.NewWebsocketPeer(conn, &serialize.JSONSerializer{}, websocket.TextMessage, log.Default(), keepAlive, 16)

	peer.Send() <- &wamp.Welcome{ID: 1, Details: wamp.Dict{}}
	<-conn.writing
	for _, msg := range queued {
		peer.Send() <- msg
	}

	closed := make(chan struct{})
	go func() {
		peer.Close()
		close(closed)
	}()
	// Close must be waiting on the held write, not done without it.
	select {
	case <-closed:
		t.Fatal("Close returned while a write was still in progress")
	case <-time.After(20 * time.Millisecond):
	}
	close(conn.release)
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not return")
	}
}

// Close writes the frames already queued, up to and including the GOODBYE,
// instead of discarding them. Before, a GOODBYE the router queued right
// before closing the session was lost whenever other frames were still
// queued ahead of it, i.e. under load.
func TestWSCloseFlushesQueuedGoodbye(t *testing.T) {
	for _, keepAlive := range []time.Duration{0, time.Hour} {
		t.Run(keepAlive.String(), func(t *testing.T) {
			conn := newHeldWriteConn()
			closeWithQueue(t, conn, keepAlive,
				&wamp.Event{Subscription: 1, Publication: 2, Details: wamp.Dict{}},
				&wamp.Goodbye{Reason: wamp.ErrGoodbyeAndOut, Details: wamp.Dict{}},
				&wamp.Event{Subscription: 1, Publication: 3, Details: wamp.Dict{}},
			)
			require.Equal(t,
				[]wamp.MessageType{wamp.WELCOME, wamp.EVENT, wamp.GOODBYE},
				conn.types(t))
		})
	}
}

// The flush shares one write deadline across all its frames rather than
// giving each a fresh one, so a slow reader cannot stretch Close to one
// writeTimeout per queued frame.
func TestWSCloseFlushSharesOneDeadline(t *testing.T) {
	conn := newHeldWriteConn()
	before := time.Now()
	closeWithQueue(t, conn, 0,
		&wamp.Event{Subscription: 1, Publication: 2, Details: wamp.Dict{}},
		&wamp.Event{Subscription: 1, Publication: 3, Details: wamp.Dict{}},
		&wamp.Goodbye{Reason: wamp.ErrGoodbyeAndOut, Details: wamp.Dict{}},
	)
	require.Len(t, conn.types(t), 4)

	deadlines := conn.recordedDeadlines()
	require.Len(t, deadlines, 4, "one deadline per data frame")
	flushed := deadlines[1:]
	for _, d := range flushed {
		require.Equal(t, flushed[0], d, "flushed frames must share one deadline")
	}
	require.True(t, flushed[0].After(before.Add(time.Second)), "deadline %v is not in the future", flushed[0])
	require.True(t, flushed[0].Before(before.Add(10*time.Minute)), "deadline %v is unbounded", flushed[0])
}

// When the remote end closes the connection first, there is nobody left to
// flush to: Close must not wait for a deadline.
func TestWSCloseAfterRemoteCloseDoesNotFlush(t *testing.T) {
	conn := newFrameRecordingConn()
	peer := transport.NewWebsocketPeer(conn, &serialize.JSONSerializer{}, websocket.TextMessage, log.Default(), 0, 16)
	_ = conn.Close() // ReadMessage fails: the remote end is gone.
	<-peer.Recv()    // recvHandler has stopped the sender.

	start := time.Now()
	peer.Close()
	require.Less(t, time.Since(start), time.Second)
}

// deadlineConn records the write deadlines set on a net.Conn.
type deadlineConn struct {
	net.Conn
	mu        sync.Mutex
	deadlines []time.Time
}

func (c *deadlineConn) SetWriteDeadline(t time.Time) error {
	c.mu.Lock()
	c.deadlines = append(c.deadlines, t)
	c.mu.Unlock()
	return c.Conn.SetWriteDeadline(t)
}

func (c *deadlineConn) recorded() []time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]time.Time(nil), c.deadlines...)
}

// readRawFrames reads WAMP frames from a rawsocket client connection until
// it is closed, and returns their types.
func readRawFrames(t *testing.T, conn net.Conn) []wamp.MessageType {
	t.Helper()
	ser := &serialize.JSONSerializer{}
	var got []wamp.MessageType
	// Set once: net.Pipe refuses SetReadDeadline after the remote end closed.
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(5*time.Second)))
	for {
		var header [4]byte
		if _, err := io.ReadFull(conn, header[:]); err != nil {
			require.ErrorIs(t, err, io.EOF, "connection not closed by the peer")
			return got
		}
		body := make([]byte, int(header[1])<<16|int(header[2])<<8|int(header[3]))
		_, err := io.ReadFull(conn, body)
		require.NoError(t, err)
		msg, err := ser.Deserialize(body)
		require.NoError(t, err)
		got = append(got, msg.MessageType())
	}
}

// The rawsocket peer flushes like the websocket peer: Close writes the frames
// already queued, up to and including the GOODBYE, under a single write
// deadline. net.Pipe has no buffer, so the first frame stays on the wire
// until the client reads and everything after it is still queued at Close.
func TestRawSocketCloseFlushesQueuedGoodbye(t *testing.T) {
	client, server := net.Pipe()
	t.Cleanup(func() { _ = client.Close() })
	conn := &deadlineConn{Conn: server}
	peer := acceptPipedRawSocket(t, client, conn, 1, 16)

	peer.Send() <- &wamp.Welcome{ID: 1, Details: wamp.Dict{}}
	peer.Send() <- &wamp.Event{Subscription: 1, Publication: 2, Details: wamp.Dict{}}
	peer.Send() <- &wamp.Goodbye{Reason: wamp.ErrGoodbyeAndOut, Details: wamp.Dict{}}
	peer.Send() <- &wamp.Event{Subscription: 1, Publication: 3, Details: wamp.Dict{}}

	before := time.Now()
	closed := make(chan struct{})
	go func() {
		peer.Close()
		close(closed)
	}()
	select {
	case <-closed:
		t.Fatal("Close returned before the queued frames were read")
	case <-time.After(20 * time.Millisecond):
	}

	require.Equal(t,
		[]wamp.MessageType{wamp.WELCOME, wamp.EVENT, wamp.GOODBYE},
		readRawFrames(t, client))
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not return")
	}

	deadlines := conn.recorded()
	require.Len(t, deadlines, 1, "flush sets one deadline for all its frames")
	require.True(t, deadlines[0].After(before.Add(time.Second)), "deadline %v is not in the future", deadlines[0])
	require.True(t, deadlines[0].Before(before.Add(10*time.Minute)), "deadline %v is unbounded", deadlines[0])
}

// When the remote end closes first, Close neither flushes nor waits.
func TestRawSocketCloseAfterRemoteCloseDoesNotFlush(t *testing.T) {
	client, server := net.Pipe()
	peer := acceptPipedRawSocket(t, client, server, 1, 16)
	_ = client.Close()
	for range peer.Recv() { //nolint:revive // drain until recvHandler exits
	}

	start := time.Now()
	peer.Close()
	require.Less(t, time.Since(start), time.Second)
}
