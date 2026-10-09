package router //nolint:testpackage

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ironflock/nexus/v3/wamp"
)

// TestKillSessionsByAuthroleGoAPI covers the Go-level
// Router.KillSessionsByAuthrole added so an embedder can revoke a role without
// enabling meta-kill on the realm (which would hand every client on that realm
// a session-kill button). Unlike the authid variant the GOODBYE reason is
// caller-supplied: clients treat wamp.close.normal as a clean shutdown and
// wind their reconnect backoff up, so a revocation must be distinguishable.
func TestKillSessionsByAuthroleGoAPI(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := newTestRouter(t)
		defer r.Close()

		// Every test session joins the open testRealm with the same authrole.
		keep := testClient(t, r)
		victim1 := testClient(t, r)
		victim2 := testClient(t, r)

		role, _ := wamp.AsString(keep.Details["authrole"])
		require.NotEmpty(t, role, "test sessions must carry an authrole")

		const reason = wamp.URI("ironflock.close.access_revoked")

		// Unknown realm → -1, nothing killed.
		require.Equal(t, -1,
			r.KillSessionsByAuthrole("no.such.realm", role, reason, "revoked", 0))

		killed := r.KillSessionsByAuthrole(testRealm, role, reason, "revoked", keep.ID)
		require.Equal(t, 2, killed, "expected both non-excluded sessions killed")

		for i, v := range []*wamp.Session{victim1, victim2} {
			msg, err := wamp.RecvTimeout(v, time.Second)
			require.NoErrorf(t, err, "victim %d should receive GOODBYE", i+1)
			g, ok := msg.(*wamp.Goodbye)
			require.Truef(t, ok, "victim %d expected GOODBYE", i+1)
			require.Equalf(t, reason, g.Reason,
				"victim %d must receive the caller-supplied revocation reason, not CloseNormal", i+1)
		}

		// The excluded session is untouched.
		_, err := wamp.RecvTimeout(keep, time.Millisecond)
		require.Error(t, err, "excluded session must not be killed")

		// A non-matching role kills nothing.
		require.Equal(t, 0,
			r.KillSessionsByAuthrole(testRealm, "no_such_role", reason, "revoked", 0))

		// An empty reason falls back to CloseNormal rather than sending an
		// invalid GOODBYE. Two sessions match now: `last`, plus `keep` which
		// survived the excluded kill above.
		last := testClient(t, r)
		require.Equal(t, 2, r.KillSessionsByAuthrole(testRealm, role, "", "", 0))
		msg, err := wamp.RecvTimeout(last, time.Second)
		require.NoError(t, err)
		g, ok := msg.(*wamp.Goodbye)
		require.True(t, ok)
		require.Equal(t, wamp.CloseNormal, g.Reason)
	})
}
