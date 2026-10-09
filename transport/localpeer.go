package transport

import (
	"sync"

	"github.com/ironflock/nexus/v3/wamp"
)

const defaultRToCQueueSize = 64

// LinkedPeers creates two connected peers. Messages sent to one peer appear in
// the Recv of the other. This is used for connecting client sessions to the
// router.
func LinkedPeers() (wamp.Peer, wamp.Peer) {
	return LinkedPeersQSize(defaultRToCQueueSize)
}

// LinkedPeersQSize is the same as LinkedPeers with the ability to specify the
// router-to-client queue size. Specifying size 0 uses default size.
//
// Each peer is paired with an internal forwarder goroutine that owns
// the lifecycle of the partner's Recv channel. Senders write to a
// user-facing Send channel that is NEVER closed; the forwarder copies
// messages onto the partner's Recv channel and closes that channel
// on exit. Closing the user-facing Send channel directly would race
// with senders ("send on closed channel" panic); routing through the
// forwarder avoids the close-vs-send race entirely.
//
// On Close, the forwarder drains any pending messages from the
// user-facing Send channel before exiting, so the send-then-close
// idiom (e.g. router's `peer.Send() <- ABORT; peer.Close()`)
// delivers the in-flight ABORT before the partner observes EOF.
func LinkedPeersQSize(queueSize int) (wamp.Peer, wamp.Peer) {
	if queueSize == 0 {
		queueSize = defaultRToCQueueSize
	}

	// User-facing Send channels — written by callers, never closed by
	// us. cOut is unbuffered (matches the upstream client→router
	// hand-off semantic); rOut is buffered (matches the upstream
	// router→client buffered queue used to absorb slow clients).
	cOut := make(chan wamp.Message)
	rOut := make(chan wamp.Message, queueSize)

	// Partner-facing Recv channels — solely written and closed by
	// the forwarders. Both unbuffered: the queue capacity lives on
	// the rOut side (matching upstream's rToC buffer) plus one
	// in-flight slot in the forwarder. Total r→c capacity is
	// therefore queueSize+1 (vs upstream's queueSize); the +1 is
	// the unavoidable cost of routing through the forwarder.
	cIn := make(chan wamp.Message)
	rIn := make(chan wamp.Message)

	cDone := make(chan struct{})
	rDone := make(chan struct{})

	// c-side forwarder: cOut → rIn. Closes rIn on exit so r's Recv
	// consumers see EOF.
	go forwardLocalPeer(cOut, rIn, cDone, rDone)
	// r-side forwarder: rOut → cIn. Closes cIn on exit so c's Recv
	// consumers see EOF.
	go forwardLocalPeer(rOut, cIn, rDone, cDone)

	c := &localPeer{out: cOut, in: cIn, done: cDone}
	r := &localPeer{out: rOut, in: rIn, done: rDone}
	return c, r
}

// forwardLocalPeer copies messages from `in` (a peer's user-facing
// Send channel) to `out` (the partner's user-facing Recv channel)
// until either side closes its Done. It defers close(out) so the
// partner's Recv consumers see EOF when this side shuts down.
//
// On selfDone, the forwarder drains any pending messages from `in`
// and forwards them before closing `out`. That preserves the
// send-then-close idiom — the router's AttachClient pattern of
// `peer.Send() <- ABORT; peer.Close()` delivers ABORT before the
// partner sees Recv EOF.
//
// Forwarder is the sole writer-and-closer of `out`; it never closes
// `in`. That asymmetry is the architectural fix for the close-vs-
// send race: closing `in` would race with senders, but `in` is never
// closed.
func forwardLocalPeer(in <-chan wamp.Message, out chan<- wamp.Message, selfDone, partnerDone <-chan struct{}) {
	defer close(out)
	// forward delivers msg to out, blocking until the partner reads
	// OR the partner is gone (partnerDone). We do NOT exit on
	// selfDone here — that would drop the send-then-close idiom
	// (e.g. the router's `peer.Send() <- ABORT; peer.Close()`,
	// where the partner may not be reading at the precise instant
	// of close). The partner is responsible for reading what's
	// addressed to it; if it stops, it must close its own peer to
	// make partnerDone fire and let the forwarder unstick.
	forward := func(msg wamp.Message) bool {
		select {
		case out <- msg:
			return true
		case <-partnerDone:
			return false
		}
	}
	for {
		select {
		case msg := <-in:
			if !forward(msg) {
				return
			}
		case <-selfDone:
			// Drain pending messages, then exit. Each delivery
			// blocks until the partner reads OR partnerDone fires.
			// Tests that don't read pending messages and don't
			// close their localSide will park here — that is a
			// test bug, not a forwarder bug. The cleanup contract
			// is: whoever holds a peer must close it.
			for {
				select {
				case msg := <-in:
					if !forward(msg) {
						return
					}
				default:
					return
				}
			}
		case <-partnerDone:
			return
		}
	}
}

// localPeer implements Peer for in-process communication. The two
// peers in a LinkedPeers pair are independent: closing one does not
// close the other directly. The partner observes closure through its
// Recv channel being closed by this side's forwarder on exit.
type localPeer struct {
	out  chan<- wamp.Message // user-facing Send; never closed
	in   <-chan wamp.Message // user-facing Recv; closed by partner's forwarder
	done chan struct{}

	closeOnce sync.Once
}

// IsLocal returns true is the wamp.Peer is a localPeer.
func (p *localPeer) IsLocal() bool { return true }

// Recv returns the channel this peer reads incoming messages from.
// The channel is closed by the partner peer's forwarder when the
// partner has finished sending.
func (p *localPeer) Recv() <-chan wamp.Message { return p.in }

// Send returns the peer's outbound message channel. The channel is
// owned and read by an internal forwarder goroutine; it is never
// closed. Senders that may race with Close should select on Done()
// to detect closure cooperatively (see wamp.Peer.Send doc).
func (p *localPeer) Send() chan<- wamp.Message { return p.out }

// Done returns a channel that is closed when Close is called.
func (p *localPeer) Done() <-chan struct{} { return p.done }

// Close signals the peer is closing. Idempotent and safe to call
// concurrently with Send. The partner peer observes closure through
// its Recv channel being closed once this side's forwarder exits
// (after draining any in-flight messages from the Send channel).
func (p *localPeer) Close() {
	p.closeOnce.Do(func() {
		close(p.done)
	})
}
