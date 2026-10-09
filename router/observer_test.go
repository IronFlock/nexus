package router //nolint:testpackage

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ironflock/nexus/v3/wamp"
)

// obsEvent records one observer callback.
type obsEvent struct {
	added  bool
	uri    wamp.URI
	match  string
	invoke string
}

// obsRecv waits for the next observer event.
func obsRecv(t *testing.T, ch <-chan obsEvent) obsEvent {
	t.Helper()
	select {
	case ev := <-ch:
		return ev
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for observer event")
		return obsEvent{}
	}
}

// obsQuiet asserts no observer event arrives within the window.
func obsQuiet(t *testing.T, ch <-chan obsEvent) {
	t.Helper()
	select {
	case ev := <-ch:
		t.Fatalf("unexpected observer event: %+v", ev)
	case <-time.After(200 * time.Millisecond):
	}
}

// recvUntil drains router messages on the session until one of type T
// arrives (skipping interleaved meta-driven messages).
func recvMsg(t *testing.T, sess *wamp.Session) wamp.Message {
	t.Helper()
	select {
	case msg := <-sess.Recv():
		return msg
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for router message")
		return nil
	}
}

func TestSubscriptionObserverLifecycle(t *testing.T) {
	const realmURI = wamp.URI("nexus.test.observer.sub")
	events := make(chan obsEvent, 64)

	r, err := NewRouter(&Config{
		RealmConfigs: []*RealmConfig{{
			URI:           realmURI,
			AnonymousAuth: true,
			SubscriptionObserver: func(added bool, topic wamp.URI, match string) {
				events <- obsEvent{added: added, uri: topic, match: match}
			},
		}},
	}, logger)
	require.NoError(t, err)
	t.Cleanup(func() { r.Close() })

	sub1 := testClientInRealm(t, r, realmURI)
	sub2 := testClientInRealm(t, r, realmURI)

	// First subscriber on a topic: added=true.
	sub1.Send() <- &wamp.Subscribe{Request: wamp.GlobalID(), Topic: "obs.topic"}
	subscribed, ok := recvMsg(t, sub1).(*wamp.Subscribed)
	require.True(t, ok)
	ev := obsRecv(t, events)
	require.Equal(t, obsEvent{added: true, uri: "obs.topic", match: wamp.MatchExact}, ev)

	// Second subscriber on the same topic: no event.
	sub2.Send() <- &wamp.Subscribe{Request: wamp.GlobalID(), Topic: "obs.topic"}
	_, ok = recvMsg(t, sub2).(*wamp.Subscribed)
	require.True(t, ok)
	obsQuiet(t, events)

	// Pattern subscriptions report their match policy.
	sub1.Send() <- &wamp.Subscribe{Request: wamp.GlobalID(), Topic: "obs.pfx",
		Options: wamp.Dict{wamp.OptMatch: wamp.MatchPrefix}}
	_, ok = recvMsg(t, sub1).(*wamp.Subscribed)
	require.True(t, ok)
	ev = obsRecv(t, events)
	require.Equal(t, obsEvent{added: true, uri: "obs.pfx", match: wamp.MatchPrefix}, ev)

	// First subscriber unsubscribes from the shared topic: still one
	// subscriber left, no event.
	sub1.Send() <- &wamp.Unsubscribe{Request: wamp.GlobalID(), Subscription: subscribed.Subscription}
	_, ok = recvMsg(t, sub1).(*wamp.Unsubscribed)
	require.True(t, ok)
	obsQuiet(t, events)

	// Last subscriber disconnects: added=false for the shared topic.
	sub2.Send() <- &wamp.Goodbye{Reason: wamp.CloseNormal, Details: wamp.Dict{}}
	ev = obsRecv(t, events)
	require.Equal(t, obsEvent{added: false, uri: "obs.topic", match: wamp.MatchExact}, ev)
}

func TestSubscriptionObserverEventHistoryTopics(t *testing.T) {
	const realmURI = wamp.URI("nexus.test.observer.history")
	events := make(chan obsEvent, 16)

	r, err := NewRouter(&Config{
		RealmConfigs: []*RealmConfig{{
			URI:           realmURI,
			AnonymousAuth: true,
			TopicEventHistoryConfigs: []*TopicEventHistoryConfig{
				{Topic: "history.topic", MatchPolicy: wamp.MatchExact, Limit: 8},
			},
			SubscriptionObserver: func(added bool, topic wamp.URI, match string) {
				events <- obsEvent{added: added, uri: topic, match: match}
			},
		}},
	}, logger)
	require.NoError(t, err)
	t.Cleanup(func() { r.Close() })

	// The event-history subscription created at realm construction is
	// reported: it is real local interest (remote events must reach the
	// history store).
	ev := obsRecv(t, events)
	require.Equal(t, obsEvent{added: true, uri: "history.topic", match: wamp.MatchExact}, ev)
}

func TestRegistrationObserverLifecycle(t *testing.T) {
	const realmURI = wamp.URI("nexus.test.observer.reg")
	events := make(chan obsEvent, 64)

	r, err := NewRouter(&Config{
		RealmConfigs: []*RealmConfig{{
			URI:           realmURI,
			AnonymousAuth: true,
			RegistrationObserver: func(added bool, proc wamp.URI, match, invoke string) {
				events <- obsEvent{added: added, uri: proc, match: match, invoke: invoke}
			},
		}},
	}, logger)
	require.NoError(t, err)
	t.Cleanup(func() { r.Close() })

	callee1 := testClientInRealm(t, r, realmURI)
	callee2 := testClientInRealm(t, r, realmURI)

	// First callee: added=true with the invocation policy.
	callee1.Send() <- &wamp.Register{Request: wamp.GlobalID(), Procedure: "obs.proc",
		Options: wamp.Dict{wamp.OptInvoke: wamp.InvokeRoundRobin}}
	registered, ok := recvMsg(t, callee1).(*wamp.Registered)
	require.True(t, ok)
	ev := obsRecv(t, events)
	require.Equal(t, obsEvent{added: true, uri: "obs.proc", match: wamp.MatchExact,
		invoke: wamp.InvokeRoundRobin}, ev)

	// Second callee joins the shared registration: no event.
	callee2.Send() <- &wamp.Register{Request: wamp.GlobalID(), Procedure: "obs.proc",
		Options: wamp.Dict{wamp.OptInvoke: wamp.InvokeRoundRobin}}
	_, ok = recvMsg(t, callee2).(*wamp.Registered)
	require.True(t, ok)
	obsQuiet(t, events)

	// First callee unregisters: one callee left, no event.
	callee1.Send() <- &wamp.Unregister{Request: wamp.GlobalID(), Registration: registered.Registration}
	_, ok = recvMsg(t, callee1).(*wamp.Unregistered)
	require.True(t, ok)
	obsQuiet(t, events)

	// Last callee disconnects: added=false.
	callee2.Send() <- &wamp.Goodbye{Reason: wamp.CloseNormal, Details: wamp.Dict{}}
	ev = obsRecv(t, events)
	require.Equal(t, obsEvent{added: false, uri: "obs.proc", match: wamp.MatchExact,
		invoke: wamp.InvokeRoundRobin}, ev)
}

func TestRegistrationObserverForceReregister(t *testing.T) {
	const realmURI = wamp.URI("nexus.test.observer.evict")
	events := make(chan obsEvent, 16)

	r, err := NewRouter(&Config{
		RealmConfigs: []*RealmConfig{{
			URI:           realmURI,
			AnonymousAuth: true,
			RegistrationObserver: func(added bool, proc wamp.URI, match, invoke string) {
				events <- obsEvent{added: added, uri: proc, match: match, invoke: invoke}
			},
		}},
	}, logger)
	require.NoError(t, err)
	t.Cleanup(func() { r.Close() })

	callee1 := testClientInRealm(t, r, realmURI)
	callee2 := testClientInRealm(t, r, realmURI)

	callee1.Send() <- &wamp.Register{Request: wamp.GlobalID(), Procedure: "evict.proc"}
	_, ok := recvMsg(t, callee1).(*wamp.Registered)
	require.True(t, ok)
	ev := obsRecv(t, events)
	require.True(t, ev.added)

	// force_reregister evicts the existing registration and installs the
	// new one: the observer must see a normalized delete then an add —
	// never the internal empty-string match.
	callee2.Send() <- &wamp.Register{Request: wamp.GlobalID(), Procedure: "evict.proc",
		Options: wamp.Dict{wamp.OptForceReregister: true}}
	_, ok = recvMsg(t, callee2).(*wamp.Registered)
	require.True(t, ok)
	ev = obsRecv(t, events)
	require.Equal(t, obsEvent{added: false, uri: "evict.proc", match: wamp.MatchExact,
		invoke: wamp.InvokeSingle}, ev)
	ev = obsRecv(t, events)
	require.True(t, ev.added)
	require.Equal(t, wamp.MatchExact, ev.match)
}
