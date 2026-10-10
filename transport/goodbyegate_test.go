package transport_test

import (
	"io"
	"log"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"

	"github.com/ironflock/nexus/v3/transport"
	"github.com/ironflock/nexus/v3/transport/serialize"
	"github.com/ironflock/nexus/v3/wamp"
)

// frameRecordingConn records every data frame written to it. ReadMessage
// blocks until Close so the peer stays up for the duration of the test.
type frameRecordingConn struct {
	mu        sync.Mutex
	frames    [][]byte
	closed    chan struct{}
	closeOnce sync.Once
}

func newFrameRecordingConn() *frameRecordingConn {
	return &frameRecordingConn{closed: make(chan struct{})}
}

func (m *frameRecordingConn) WriteMessage(_ int, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.frames = append(m.frames, append([]byte(nil), data...))
	return nil
}

func (m *frameRecordingConn) types(t *testing.T) []wamp.MessageType {
	t.Helper()
	m.mu.Lock()
	defer m.mu.Unlock()
	ser := &serialize.JSONSerializer{}
	out := make([]wamp.MessageType, 0, len(m.frames))
	for _, b := range m.frames {
		msg, err := ser.Deserialize(b)
		require.NoError(t, err)
		out = append(out, msg.MessageType())
	}
	return out
}

func (m *frameRecordingConn) Close() error {
	m.closeOnce.Do(func() { close(m.closed) })
	return nil
}
func (m *frameRecordingConn) WriteControl(int, []byte, time.Time) error { return nil }
func (m *frameRecordingConn) ReadMessage() (int, []byte, error) {
	<-m.closed
	return 0, nil, websocket.ErrCloseSent
}
func (m *frameRecordingConn) SetPongHandler(func(string) error) {}
func (m *frameRecordingConn) SetPingHandler(func(string) error) {}
func (m *frameRecordingConn) Subprotocol() string               { return "" }

// afterGoodbyeSequence is what a router may queue for a session that is
// leaving: its GOODBYE, then an EVENT routed before the session was removed
// from the broker, a reply GOODBYE, an ABORT, and finally a new session on
// the same transport.
func afterGoodbyeSequence() []wamp.Message {
	return []wamp.Message{
		&wamp.Goodbye{Reason: wamp.ErrGoodbyeAndOut, Details: wamp.Dict{}},
		&wamp.Event{Subscription: 1, Publication: 2, Details: wamp.Dict{}},
		&wamp.Goodbye{Reason: wamp.CloseNormal, Details: wamp.Dict{}},
		&wamp.Abort{Reason: wamp.ErrProtocolViolation, Details: wamp.Dict{}},
		&wamp.Welcome{ID: 3, Details: wamp.Dict{}},
		&wamp.Event{Subscription: 1, Publication: 4, Details: wamp.Dict{}},
	}
}

// wantAfterGoodbye is the wire order for afterGoodbyeSequence: nothing but
// ABORT between the GOODBYE and the next WELCOME.
var wantAfterGoodbye = []wamp.MessageType{wamp.GOODBYE, wamp.ABORT, wamp.WELCOME, wamp.EVENT}

// A websocket peer writes nothing after its own GOODBYE, with and without
// keepalive (the two send loops). Regression test for gammazero/nexus#347.
func TestWSNoSendAfterGoodbye(t *testing.T) {
	for _, keepAlive := range []time.Duration{0, time.Hour} {
		t.Run(keepAlive.String(), func(t *testing.T) {
			conn := newFrameRecordingConn()
			peer := transport.NewWebsocketPeer(conn, &serialize.JSONSerializer{}, websocket.TextMessage, log.Default(), keepAlive, 0)
			t.Cleanup(peer.Close)

			for _, msg := range afterGoodbyeSequence() {
				peer.Send() <- msg
			}
			require.Eventually(t, func() bool { return len(conn.types(t)) >= len(wantAfterGoodbye) },
				time.Second, 5*time.Millisecond)
			// Give a wrongly forwarded frame time to show up.
			time.Sleep(20 * time.Millisecond)
			require.Equal(t, wantAfterGoodbye, conn.types(t))
		})
	}
}

// Same rule for the rawsocket transport.
func TestRawSocketNoSendAfterGoodbye(t *testing.T) {
	client, peer := newPipedRawSocket(t, 1)
	t.Cleanup(func() {
		_ = client.Close()
		peer.Close()
	})

	go func() {
		for _, msg := range afterGoodbyeSequence() {
			peer.Send() <- msg
		}
	}()

	ser := &serialize.JSONSerializer{}
	var got []wamp.MessageType
	for len(got) < len(wantAfterGoodbye) {
		require.NoError(t, client.SetReadDeadline(time.Now().Add(time.Second)))
		var header [4]byte
		_, err := io.ReadFull(client, header[:])
		require.NoError(t, err)
		body := make([]byte, int(header[1])<<16|int(header[2])<<8|int(header[3]))
		_, err = io.ReadFull(client, body)
		require.NoError(t, err)
		msg, err := ser.Deserialize(body)
		require.NoError(t, err)
		got = append(got, msg.MessageType())
	}
	require.Equal(t, wantAfterGoodbye, got)
}
