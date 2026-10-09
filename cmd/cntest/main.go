// Command cntest is a throwaway cross-node validation client for the
// IronFlock test cluster. It connects two anonymous WAMP clients to two
// DIFFERENT router pods (via port-forward URLs) and verifies pub/sub and
// RPC cross the mesh in both directions. Not part of the build (deleted
// after use).
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/ironflock/nexus/v3/client"
	"github.com/ironflock/nexus/v3/wamp"
)

func connect(url string) (*client.Client, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	return client.ConnectNet(ctx, url, client.Config{
		Realm:           "realm1",
		Serialization:   client.JSON,
		ResponseTimeout: 5 * time.Second,
		Logger:          log.New(io.Discard, "", 0),
	})
}

func main() {
	if len(os.Args) >= 3 && os.Args[1] == "drainwatch" {
		drainWatch(os.Args[2])
		return
	}
	if len(os.Args) < 3 {
		log.Fatal("usage: cntest <urlA> <urlB>  |  cntest drainwatch <url>")
	}
	a, err := connect(os.Args[1])
	if err != nil {
		log.Fatalf("connect A (%s): %v", os.Args[1], err)
	}
	defer a.Close()
	b, err := connect(os.Args[2])
	if err != nil {
		log.Fatalf("connect B (%s): %v", os.Args[2], err)
	}
	defer b.Close()
	fmt.Printf("connected: A session=%d  B session=%d\n", a.ID(), b.ID())

	fails := 0
	if !pubsub(a, b, "ct.pubsub.ab") { // sub on A, pub on B
		fails++
	}
	if !pubsub(b, a, "ct.pubsub.ba") { // sub on B, pub on A
		fails++
	}
	if !rpc(a, b, "ct.rpc.ab") { // callee A, caller B
		fails++
	}
	if !rpc(b, a, "ct.rpc.ba") { // callee B, caller A
		fails++
	}
	if fails > 0 {
		fmt.Printf("\nRESULT: FAILED (%d/4)\n", fails)
		os.Exit(1)
	}
	fmt.Println("\nRESULT: ALL 4 CROSS-NODE TESTS PASSED")
}

// drainWatch connects one client and blocks until the router disconnects
// it, reporting whether a clean GOODBYE (and its reason) was received —
// the client-facing signal of a graceful drain.
func drainWatch(url string) {
	c, err := connect(url)
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	_ = c.Subscribe("ct.drain.keepalive", func(*wamp.Event) {}, nil)
	fmt.Printf("connected session=%d — waiting for drain/disconnect...\n", c.ID())
	start := time.Now()
	<-c.Done()
	el := time.Since(start).Round(time.Millisecond)
	if gb := c.RouterGoodbye(); gb != nil {
		fmt.Printf("DISCONNECTED after %s — router GOODBYE reason=%q\n", el, gb.Reason)
	} else {
		fmt.Printf("DISCONNECTED after %s — no GOODBYE (raw transport drop)\n", el)
	}
}

func pubsub(sub, pub *client.Client, topic string) bool {
	got := make(chan *wamp.Event, 8)
	if err := sub.Subscribe(topic, func(e *wamp.Event) { got <- e }, nil); err != nil {
		fmt.Printf("[pubsub %-13s] FAIL subscribe: %v\n", topic, err)
		return false
	}
	defer func() { _ = sub.Unsubscribe(topic) }()
	want := "hello-" + topic
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		// Acknowledged publish so we know the router accepted it; retry to
		// ride out the ms-scale interest-propagation window across the mesh.
		if err := pub.Publish(topic, wamp.Dict{"acknowledge": true}, wamp.List{want}, nil); err != nil {
			fmt.Printf("[pubsub %-13s] FAIL publish: %v\n", topic, err)
			return false
		}
		select {
		case e := <-got:
			if len(e.Arguments) == 1 && e.Arguments[0] == want {
				fmt.Printf("[pubsub %-13s] PASS — cross-node event delivered\n", topic)
				return true
			}
		case <-time.After(750 * time.Millisecond):
		}
	}
	fmt.Printf("[pubsub %-13s] FAIL — event not delivered cross-node within timeout\n", topic)
	return false
}

func rpc(callee, caller *client.Client, proc string) bool {
	if err := callee.Register(proc, func(_ context.Context, inv *wamp.Invocation) client.InvokeResult {
		var arg interface{}
		if len(inv.Arguments) > 0 {
			arg = inv.Arguments[0]
		}
		return client.InvokeResult{Args: wamp.List{fmt.Sprintf("echo:%v", arg)}}
	}, nil); err != nil {
		fmt.Printf("[rpc    %-13s] FAIL register: %v\n", proc, err)
		return false
	}
	defer func() { _ = callee.Unregister(proc) }()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		res, err := caller.Call(ctx, proc, nil, wamp.List{"ping"}, nil, nil)
		cancel()
		if err == nil {
			if len(res.Arguments) == 1 && res.Arguments[0] == "echo:ping" {
				fmt.Printf("[rpc    %-13s] PASS — cross-node call returned %q\n", proc, res.Arguments[0])
				return true
			}
			fmt.Printf("[rpc    %-13s] FAIL unexpected result: %v\n", proc, res.Arguments)
			return false
		}
		var rpce client.RPCError
		if errors.As(err, &rpce) && rpce.Err.Error == wamp.ErrNoSuchProcedure {
			time.Sleep(300 * time.Millisecond) // interest still propagating
			continue
		}
		fmt.Printf("[rpc    %-13s] FAIL call: %v\n", proc, err)
		return false
	}
	fmt.Printf("[rpc    %-13s] FAIL — no_such_procedure persisted (interest never propagated)\n", proc)
	return false
}
