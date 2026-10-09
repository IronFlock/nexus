package router //nolint:testpackage

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ironflock/nexus/v3/wamp"
)

// TestKillSessionsByAuthidGoAPI covers the Go-level Router.KillSessionsByAuthid
// added for embedders (the cluster mesh) to enforce single-authority
// identities: it closes matching sessions except the excluded one, returns the
// kill count, and reports -1 for an unknown realm.
func TestKillSessionsByAuthidGoAPI(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := newTestRouter(t)
		defer r.Close()

		// testClient gives every session authid "user1".
		keep := testClient(t, r)
		victim1 := testClient(t, r)
		victim2 := testClient(t, r)

		// Unknown realm → -1, nothing killed.
		require.Equal(t, -1, r.KillSessionsByAuthid("no.such.realm", "user1", 0))

		// Kill all "user1" sessions except `keep`.
		killed := r.KillSessionsByAuthid(testRealm, "user1", keep.ID)
		require.Equal(t, 2, killed, "expected both non-excluded sessions killed")

		for i, v := range []*wamp.Session{victim1, victim2} {
			msg, err := wamp.RecvTimeout(v, time.Second)
			require.NoErrorf(t, err, "victim %d should receive GOODBYE", i+1)
			g, ok := msg.(*wamp.Goodbye)
			require.Truef(t, ok, "victim %d expected GOODBYE", i+1)
			require.Equal(t, wamp.CloseNormal, g.Reason)
		}

		// The excluded session is untouched.
		_, err := wamp.RecvTimeout(keep, time.Millisecond)
		require.Error(t, err, "excluded session must not be killed")

		// A non-matching authid kills nothing.
		require.Equal(t, 0, r.KillSessionsByAuthid(testRealm, "nobody", 0))
	})
}
