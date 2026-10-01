package client_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gammazero/nexus/v3/client"
	"github.com/gammazero/nexus/v3/wamp"
)

// deadWriterPeer models a websocket peer whose writer has gone: Send() is
// unbuffered and, once the join handshake has been answered, nothing drains
// it. This is what a client sees after its sendHandler exited on a write
// error while a request was on its way out.
type deadWriterPeer struct {
	send      chan wamp.Message
	recv      chan wamp.Message
	done      chan struct{}
	closeOnce sync.Once
	goneOnce  sync.Once
}

func newDeadWriterPeer() *deadWriterPeer {
	p := &deadWriterPeer{
		send: make(chan wamp.Message),
		recv: make(chan wamp.Message),
		done: make(chan struct{}),
	}
	// Answer the HELLO of the join handshake, then stop reading forever.
	go func() {
		<-p.send
		p.recv <- &wamp.Welcome{
			ID: 1,
			Details: wamp.Dict{
				"roles": wamp.Dict{"broker": wamp.Dict{}, "dealer": wamp.Dict{}},
			},
		}
	}()
	return p
}

func (p *deadWriterPeer) Close()                    { p.closeOnce.Do(func() { close(p.done) }) }
func (p *deadWriterPeer) IsLocal() bool             { return false }
func (p *deadWriterPeer) Recv() <-chan wamp.Message { return p.recv }
func (p *deadWriterPeer) Send() chan<- wamp.Message { return p.send }
func (p *deadWriterPeer) Done() <-chan struct{}     { return p.done }

// remoteGone ends the read side, as a broken socket does; the client's run
// loop exits and its context is cancelled.
func (p *deadWriterPeer) remoteGone() { p.goneOnce.Do(func() { close(p.recv) }) }

// A CALL that cannot be handed to the writer must honour its context rather
// than park on the send forever.
func TestCallHonoursContextWhenWriterIsGone(t *testing.T) {
	peer := newDeadWriterPeer()
	c, err := client.NewClient(peer, *newTestClientConfig(testRealm))
	require.NoError(t, err)
	t.Cleanup(func() { peer.remoteGone(); _ = c.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	errc := make(chan error, 1)
	go func() {
		_, err := c.Call(ctx, "test.proc", nil, nil, nil, nil)
		errc <- err
	}()

	select {
	case err := <-errc:
		require.ErrorIs(t, err, context.DeadlineExceeded)
	case <-time.After(2 * time.Second):
		t.Fatal("Call parked on a dead writer instead of honouring its context")
	}
}

// Requests without a context (REGISTER, SUBSCRIBE, PUBLISH) parked on a dead
// writer must return once the client shuts down.
func TestRequestsReturnWhenClientDiesOnDeadWriter(t *testing.T) {
	ops := map[string]func(c *client.Client) error{
		"Register": func(c *client.Client) error {
			return c.Register("test.proc", func(context.Context, *wamp.Invocation) client.InvokeResult {
				return client.InvokeResult{}
			}, nil)
		},
		"Subscribe": func(c *client.Client) error {
			return c.Subscribe("test.topic", func(*wamp.Event) {}, nil)
		},
		"Publish": func(c *client.Client) error {
			return c.Publish("test.topic", nil, nil, nil)
		},
	}
	for name, op := range ops {
		t.Run(name, func(t *testing.T) {
			peer := newDeadWriterPeer()
			c, err := client.NewClient(peer, *newTestClientConfig(testRealm))
			require.NoError(t, err)
			t.Cleanup(func() { _ = c.Close() })

			errc := make(chan error, 1)
			go func() { errc <- op(c) }()
			time.Sleep(50 * time.Millisecond) // let the request park on the send
			peer.remoteGone()

			select {
			case err := <-errc:
				require.ErrorIs(t, err, client.ErrNotConn)
			case <-time.After(2 * time.Second):
				t.Fatalf("%s stayed parked on a dead writer after the client died", name)
			}
		})
	}
}
