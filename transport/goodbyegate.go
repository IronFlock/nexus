package transport

import "github.com/ironflock/nexus/v3/wamp"

// goodbyeGate enforces, at the point where frames are written to the wire,
// that a peer sends nothing on a session after its own GOODBYE.
//
// On the router side, messages for a session reach the transport's write
// channel from many goroutines (broker, dealer, realm, meta peer), and the
// session is only removed from the broker and dealer after the realm has
// queued its GOODBYE. Events and invocations routed in that window, such as
// the wamp.registration.on_delete meta event for the leaving session's own
// registrations, would otherwise follow the GOODBYE onto the wire. Clients
// that keep reading until the transport closes (autobahn) treat that as a
// protocol violation. See gammazero/nexus#347.
//
// The send goroutine is the only place where the outbound order is decided,
// so the gate lives there and is not safe for concurrent use. ABORT always
// passes. HELLO and WELCOME open a new session on the same transport and
// reopen the gate.
type goodbyeGate struct {
	closed bool
}

// allow reports whether msg may be written, and closes the gate when msg is
// a GOODBYE.
func (g *goodbyeGate) allow(msg wamp.Message) bool {
	switch msg.MessageType() {
	case wamp.ABORT:
		return true
	case wamp.HELLO, wamp.WELCOME:
		g.closed = false
		return true
	case wamp.GOODBYE:
		if g.closed {
			return false
		}
		g.closed = true
		return true
	}
	return !g.closed
}
