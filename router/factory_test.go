package router //nolint:testpackage

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ironflock/nexus/v3/stdlog"
	"github.com/ironflock/nexus/v3/wamp"
)

// brokerFactoryProbe wraps the default broker but records that the
// factory was invoked, so the test can assert RealmConfig.BrokerFactory
// is honored when set.
type brokerFactoryProbe struct {
	Broker
	called bool
}

// dealerFactoryProbe is the analogous wrapper for Dealer.
type dealerFactoryProbe struct {
	Dealer
	called bool
}

// TestRealmConfigBrokerAndDealerFactoriesHonored verifies that custom
// BrokerFactory / DealerFactory hooks on RealmConfig are invoked
// instead of the default in-process implementations. Critical for
// downstream consumers (e.g. clustered nexus, ironflock-router) that
// need to substitute their own broker/dealer.
func TestRealmConfigBrokerAndDealerFactoriesHonored(t *testing.T) {
	const realmURI = wamp.URI("nexus.test.factory")

	var brokerProbe *brokerFactoryProbe
	var dealerProbe *dealerFactoryProbe

	r, err := NewRouter(&Config{
		RealmConfigs: []*RealmConfig{
			{
				URI:       realmURI,
				StrictURI: false,
				BrokerFactory: func(cfg *RealmConfig, logger stdlog.StdLog, debug bool) (Broker, error) {
					inner, err := defaultBrokerFactory(cfg, logger, debug)
					if err != nil {
						return nil, err
					}
					brokerProbe = &brokerFactoryProbe{Broker: inner, called: true}
					return brokerProbe, nil
				},
				DealerFactory: func(cfg *RealmConfig, logger stdlog.StdLog, debug bool) (Dealer, error) {
					inner, err := defaultDealerFactory(cfg, logger, debug)
					if err != nil {
						return nil, err
					}
					dealerProbe = &dealerFactoryProbe{Dealer: inner, called: true}
					return dealerProbe, nil
				},
			},
		},
	}, logger)
	require.NoError(t, err)
	t.Cleanup(func() { r.Close() })

	require.NotNil(t, brokerProbe, "BrokerFactory was not called")
	require.True(t, brokerProbe.called)
	require.NotNil(t, dealerProbe, "DealerFactory was not called")
	require.True(t, dealerProbe.called)
}

// TestNewDefaultBrokerAndDealerCarryTraffic verifies the exported
// default constructors produce fully functional implementations when
// used from a decorating factory — the seam downstream consumers use to
// wrap (not replace) the in-process broker/dealer. A pub/sub round trip
// proves the decorated broker dispatches, and a registration round trip
// proves the decorated dealer does.
func TestNewDefaultBrokerAndDealerCarryTraffic(t *testing.T) {
	const realmURI = wamp.URI("nexus.test.factory.exported")

	r, err := NewRouter(&Config{
		RealmConfigs: []*RealmConfig{
			{
				URI:           realmURI,
				AnonymousAuth: true,
				BrokerFactory: func(cfg *RealmConfig, logger stdlog.StdLog, debug bool) (Broker, error) {
					inner, err := NewDefaultBroker(cfg, logger, debug)
					if err != nil {
						return nil, err
					}
					return &brokerFactoryProbe{Broker: inner, called: true}, nil
				},
				DealerFactory: func(cfg *RealmConfig, logger stdlog.StdLog, debug bool) (Dealer, error) {
					inner, err := NewDefaultDealer(cfg, logger, debug)
					if err != nil {
						return nil, err
					}
					return &dealerFactoryProbe{Dealer: inner, called: true}, nil
				},
			},
		},
	}, logger)
	require.NoError(t, err)
	t.Cleanup(func() { r.Close() })

	sub := testClientInRealm(t, r, realmURI)
	pub := testClientInRealm(t, r, realmURI)

	// recv bounds every wait so a regression fails the test instead of
	// hanging it.
	recv := func(sess *wamp.Session) wamp.Message {
		t.Helper()
		select {
		case msg := <-sess.Recv():
			return msg
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for router message")
			return nil
		}
	}

	// Pub/sub through the decorated broker.
	subID := wamp.GlobalID()
	sub.Send() <- &wamp.Subscribe{Request: subID, Topic: "test.topic"}
	msg := recv(sub)
	subscribed, ok := msg.(*wamp.Subscribed)
	require.True(t, ok, "expected SUBSCRIBED, got %T", msg)
	require.Equal(t, subID, subscribed.Request)

	pub.Send() <- &wamp.Publish{Request: wamp.GlobalID(), Topic: "test.topic",
		Arguments: wamp.List{"hello"}}
	msg = recv(sub)
	event, ok := msg.(*wamp.Event)
	require.True(t, ok, "expected EVENT, got %T", msg)
	require.Equal(t, wamp.List{"hello"}, event.Arguments)

	// Registration through the decorated dealer.
	regID := wamp.GlobalID()
	sub.Send() <- &wamp.Register{Request: regID, Procedure: "test.proc"}
	msg = recv(sub)
	registered, ok := msg.(*wamp.Registered)
	require.True(t, ok, "expected REGISTERED, got %T", msg)
	require.Equal(t, regID, registered.Request)
}

// TestRealmConfigUnsetFactoriesUseDefaults exercises the fallback path
// — when neither factory is set, the realm uses the default in-process
// broker/dealer. Indirectly verified by every other test in this
// package, but pinned here so a future refactor that breaks the
// default selection fails loud at this single test.
func TestRealmConfigUnsetFactoriesUseDefaults(t *testing.T) {
	const realmURI = wamp.URI("nexus.test.factory.default")

	r, err := NewRouter(&Config{
		RealmConfigs: []*RealmConfig{
			{URI: realmURI, StrictURI: false, AnonymousAuth: true},
		},
	}, logger)
	require.NoError(t, err)
	t.Cleanup(func() { r.Close() })

	// Reach into the realm to inspect the dynamic types of broker / dealer.
	rl := r.(*router).realms[realmURI]
	require.NotNil(t, rl)
	_, isDefaultBroker := rl.broker.(*broker)
	require.True(t, isDefaultBroker, "expected default *broker; got %T", rl.broker)
	_, isDefaultDealer := rl.dealer.(*dealer)
	require.True(t, isDefaultDealer, "expected default *dealer; got %T", rl.dealer)
}
