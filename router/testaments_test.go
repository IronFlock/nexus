package router //nolint:testpackage

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ironflock/nexus/v3/wamp"
)

func TestSessionTestaments(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := newTestRouter(t)
		defer r.Close()

		sub := testClient(t, r)
		subscribeID := wamp.GlobalID()
		sub.Send() <- &wamp.Subscribe{Request: subscribeID, Topic: "testament.test1"}
		msg, err := wamp.RecvTimeout(sub, time.Second)
		require.NoError(t, err)
		_, ok := msg.(*wamp.Subscribed)
		require.True(t, ok, "expected RESULT")

		sub.Send() <- &wamp.Subscribe{Request: subscribeID, Topic: "testament.test2"}
		msg, err = wamp.RecvTimeout(sub, time.Second)
		require.NoError(t, err)
		require.NotNil(t, msg)

		caller1 := testClient(t, r)
		caller2 := testClient(t, r)

		callID := wamp.GlobalID()
		caller1.Send() <- &wamp.Call{
			Request:   callID,
			Procedure: wamp.MetaProcSessionAddTestament,
			Arguments: wamp.List{
				"testament.test1",
				wamp.List{"foo"},
				wamp.Dict{},
			},
		}

		msg, err = wamp.RecvTimeout(caller1, time.Second)
		require.NoError(t, err)
		result, ok := msg.(*wamp.Result)
		require.True(t, ok, "expected RESULT")
		require.Equal(t, callID, result.Request, "wrong result ID")

		caller2.Send() <- &wamp.Call{
			Request:   wamp.GlobalID(),
			Procedure: wamp.MetaProcSessionAddTestament,
			Arguments: wamp.List{
				"testament.test2",
				wamp.List{"foo"},
				wamp.Dict{},
			},
		}

		msg, err = wamp.RecvTimeout(caller2, time.Second)
		require.NoError(t, err)
		_, ok = msg.(*wamp.Result)
		require.True(t, ok, "expected RESULT")

		caller1.Close()
		caller2.Close()

		msg, err = wamp.RecvTimeout(sub, 5*time.Second)
		require.NoError(t, err)
		event, ok := msg.(*wamp.Event)
		require.True(t, ok, "expected EVENT")
		val, _ := wamp.AsString(event.Arguments[0])
		require.Equal(t, "foo", val, "Argument value was invalid")

		msg, err = wamp.RecvTimeout(sub, time.Second)
		require.NoError(t, err)
		event, ok = msg.(*wamp.Event)
		require.True(t, ok, "expected EVENT")
		val, _ = wamp.AsString(event.Arguments[0])
		require.Equal(t, "foo", val, "Argument value was invalid")
	})
}

func TestSessionListTestaments(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := newTestRouter(t)
		defer r.Close()

		caller1 := testClient(t, r)
		caller2 := testClient(t, r)

		// caller1 subscribes to caller2's testament topic so we can observe
		// exactly when caller2's leave has been processed (the fired EVENT).
		subID := wamp.GlobalID()
		caller1.Send() <- &wamp.Subscribe{Request: subID, Topic: "testament.t2"}
		msg0, err0 := wamp.RecvTimeout(caller1, time.Second)
		require.NoError(t, err0)
		_, ok0 := msg0.(*wamp.Subscribed)
		require.True(t, ok0, "expected SUBSCRIBED")

		// caller1 arms a destroyed-scope testament (the default scope).
		callID := wamp.GlobalID()
		caller1.Send() <- &wamp.Call{
			Request:   callID,
			Procedure: wamp.MetaProcSessionAddTestament,
			Arguments: wamp.List{"testament.t1", wamp.List{"foo"}, wamp.Dict{}},
		}
		msg, err := wamp.RecvTimeout(caller1, time.Second)
		require.NoError(t, err)
		_, ok := msg.(*wamp.Result)
		require.True(t, ok, "expected RESULT")

		// caller2 arms a detached-scope testament.
		caller2.Send() <- &wamp.Call{
			Request:     wamp.GlobalID(),
			Procedure:   wamp.MetaProcSessionAddTestament,
			Arguments:   wamp.List{"testament.t2", wamp.List{"bar"}, wamp.Dict{}},
			ArgumentsKw: wamp.Dict{"scope": "detached"},
		}
		msg, err = wamp.RecvTimeout(caller2, time.Second)
		require.NoError(t, err)
		_, ok = msg.(*wamp.Result)
		require.True(t, ok, "expected RESULT")

		// list_testaments must report both, with the right scope per topic.
		listID := wamp.GlobalID()
		caller1.Send() <- &wamp.Call{
			Request:   listID,
			Procedure: wamp.MetaProcSessionListTestaments,
		}
		msg, err = wamp.RecvTimeout(caller1, time.Second)
		require.NoError(t, err)
		result, ok := msg.(*wamp.Result)
		require.True(t, ok, "expected RESULT")
		require.Equal(t, listID, result.Request, "wrong result ID")
		require.Len(t, result.Arguments, 1)

		entries, ok := wamp.AsList(result.Arguments[0])
		require.True(t, ok, "expected list arg")
		require.Len(t, entries, 2)

		byTopic := map[string]wamp.Dict{}
		for _, e := range entries {
			d, ok := wamp.AsDict(e)
			require.True(t, ok, "expected dict entry")
			topic, _ := wamp.AsString(d["topic"])
			byTopic[topic] = d
		}

		require.Contains(t, byTopic, "testament.t1")
		require.Contains(t, byTopic, "testament.t2")
		scope1, _ := wamp.AsString(byTopic["testament.t1"]["scope"])
		require.Equal(t, "destroyed", scope1)
		scope2, _ := wamp.AsString(byTopic["testament.t2"]["scope"])
		require.Equal(t, "detached", scope2)

		// After a caller leaves, its testament drops out of the listing. Wait
		// for the fired testament EVENT first so the leave is fully processed.
		caller2.Close()
		msg, err = wamp.RecvTimeout(caller1, 5*time.Second)
		require.NoError(t, err)
		_, ok = msg.(*wamp.Event)
		require.True(t, ok, "expected EVENT from fired testament")

		listID = wamp.GlobalID()
		caller1.Send() <- &wamp.Call{
			Request:   listID,
			Procedure: wamp.MetaProcSessionListTestaments,
		}
		msg, err = wamp.RecvTimeout(caller1, time.Second)
		require.NoError(t, err)
		result, ok = msg.(*wamp.Result)
		require.True(t, ok, "expected RESULT")
		entries, ok = wamp.AsList(result.Arguments[0])
		require.True(t, ok, "expected list arg")
		require.Len(t, entries, 1)
		remaining, _ := wamp.AsDict(entries[0])
		topic, _ := wamp.AsString(remaining["topic"])
		require.Equal(t, "testament.t1", topic)
	})
}
