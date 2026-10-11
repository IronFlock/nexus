<img src="doc/n-logo2.png">

# WAMP v2 router library, client library and router service

[![Main CI](https://github.com/IronFlock/nexus/actions/workflows/main-golint.yml/badge.svg)](https://github.com/IronFlock/nexus/actions/workflows/main-golint.yml)
[![Coverage](https://img.shields.io/badge/coverage-62.8%25-orange)](https://github.com/IronFlock/nexus/actions/workflows/main-golint.yml)
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Go Reference](https://pkg.go.dev/badge/github.com/ironflock/nexus/v3.svg)](https://pkg.go.dev/github.com/ironflock/nexus/v3)

## About this fork

This is IronFlock's fork of [gammazero/nexus](https://github.com/gammazero/nexus). It is the library underneath
the IronFlock WAMP router. The module path is `github.com/ironflock/nexus/v3`. Everything else in this README
applies to both repositories unless it says otherwise.

What the fork adds on top of upstream:

- **Extension seams for a clustered router.** `Broker` and `Dealer` are interfaces. `RealmConfig.BrokerFactory` and
  `DealerFactory` let you wrap the defaults (`NewDefaultBroker`, `NewDefaultDealer`). Subscription and
  registration observers, a `SubMetaSink` for subscription meta events, and `Dealer.EvictRegistration` let an
  external layer mirror and forward routing state between nodes.
- **Operations API.** `Router.Drain()` sends GOODBYE to every client and refuses new sessions, while work already
  in flight completes. `Router.KillSessionsByAuthid` and `KillSessionsByAuthrole` close sessions from Go code.
- **Router options.** `RealmConfig.AuthorizeUnlocked` lets a slow `Authorizer` run without stalling the realm.
  `RealmConfig.StrictRequestIDs` enforces sequential request IDs (WAMP §5.1.2). Websocket servers expose TCP
  buffer and Nagle settings. Authenticators can see the target realm URI.
- **Protocol features.** `force_reregister` and `wamp.session.list_testaments`.
- **Robustness fixes.** Slow callers no longer block callees. Websocket frame writes have a deadline. Nothing is
  sent on a session after its GOODBYE, and `Close()` flushes queued frames up to the GOODBYE. Realm shutdown runs
  in order. Peer `Close` is idempotent. Several goroutine leaks are fixed. Progressive payload transport (PPT)
  round-trips over every serializer.
- **WAMP conformance harness.** [`spec/`](spec/) has YAML test plans that check the router's behaviour frame by
  frame over the wire.

Fixes that are useful outside IronFlock are offered back to gammazero/nexus as pull requests. IronFlock-specific
code stays out of this repository.

**nexus** is a [WAMP](http://wamp-proto.org/) v2 router library, client library, and a router service, that implements
most of the features defined in the WAMP-protocol advanced profile. The nexus project is written in
[Go](http://golang.org/) and designed for highly concurrent asynchronous I/O. The nexus router provides extended
functionality. The router and client interoperate with other WAMP implementations.

## Documentation

See the upstream [**nexus project Wiki**](https://github.com/gammazero/nexus/wiki) for documentation, examples, and
operational details. The wiki does not cover the fork-only additions listed above. Those are documented in the Go
doc comments.

[![Go Reference](https://pkg.go.dev/badge/github.com/ironflock/nexus/v3.svg)](https://pkg.go.dev/github.com/ironflock/nexus/v3)
for API documentation.

## What is WAMP and nexus

The Web Application Messaging Protocol (WAMP) is an open standard WebSocket subprotocol that provides two application
messaging patterns in one unified protocol:
[Remote Procedure Calls](http://wamp-proto.org/faq/index.html#rpc) and
[Publish & Subscribe](http://wamp-proto.org/faq/index.html#pubsub). WAMP is also referred to as WAMP-proto it
distinguish it from [other uses](https://en.wikipedia.org/wiki/WAMP_(disambiguation) of the acronym.

Using WAMP you can build distributed systems out of application components which are loosely coupled and communicate
in (soft) real-time.

Nexus is a software package that provides a WAMP router library, client library, and stand-alone WAMP router service.
This nexus package provides a messaging system for building distributed applications and services. Endpoints using
websockets, TCP sockets, Unix sockets, and in-process connections may all communicate with each other via pub/sub
and routed RPC messaging.

 - The router library can be used to build custom WAMP routers or to embed a WAMP router in an application.
 - The client library can be used to build clients that connect to any WAMP server, or to communicate in-process
   with a WAMP router embedded in the same application.
 - The router service can be run as-is to provide WAMP routing.

## Installation

### Install nexusd router service from source

```
go install github.com/ironflock/nexus/v3/nexusd@latest
```

Run `nexusd -help` to see usage information. You can run `nexusd` with command-line flags only, but the
[configuration file](nexusd/etc/nexus.json) gives you more options.

### Create configuration file and run nexusd service

```
git clone https://github.com/IronFlock/nexus.git
cp nexus/nexusd/etc/nexus.json ./
vi nexus.json
nexusd -c nexus.json
```

### Install examples to run and test with

```
git clone https://github.com/IronFlock/nexus.git
cd nexus/examples
```

## Building routers and clients with nexus

Use the nexus router library to build a WAMP router or to embed one in your service. Use the nexus client library
to build WAMP clients into your Go applications. Import the v3 packages of the fork's module path. v3 is the only
supported version.

```go
import (
	"github.com/ironflock/nexus/v3/client"
	"github.com/ironflock/nexus/v3/wamp"
)
```

## Run nexusd from a container

This fork does not publish a container image. The `ghcr.io/gammazero/nexusd` image is built from upstream code
and does not include the fork's changes. Build the image from this repository instead. The
[Dockerfile](Dockerfile) installs `nexusd` from the `github.com/ironflock/nexus/v3` module:

```
docker build -t nexusd .
docker run -p 8080:8080 nexusd
```

By default, the router accepts connections on port `8080` and allows anonymous clients on `realm1`. To change
these values or get help, pass options to the container process:

```
docker run nexusd --ws 0.0.0.0:8888 --realm autobahn
docker run nexusd --help
```

You can also mount a nexusd configuration file into the container for more control and a more secure setup. See
the nexusd configuration reference for details. You can also run the container process as an unprivileged user.

## Examples

The [examples](examples/) show how to create simple clients and servers. Start with
[examples/README.md](examples/README.md). The upstream
[wiki documentation](https://github.com/gammazero/nexus/wiki) has more examples of advanced profile features.

The [interop examples](examples/interop/) show nexus working with other WAMP implementations. Each one has its own
README.md that explains how to set up the environment and run it.

The [nexusd](nexusd/) service is also an example of a stand-alone WAMP router. Its configuration file sets every
router option. Use the file in `nexusd/etc/` as a starting point for your own WAMP router service.

## Contributing

Read the [Contributing](CONTRIBUTING.md#contributing-to-nexus) guide if you want to contribute.

Report problems with the fork-only additions as an [issue on this fork](https://github.com/IronFlock/nexus/issues).
Report general nexus problems and feature requests [upstream](https://github.com/gammazero/nexus/issues).

## Nexus Features

- **Concurrent Asynchronous I/O** Nexus supports large numbers of clients concurrently sending and receiving messages,
  and never blocks on I/O, even if a client becomes unresponsive. See
  [Router Concurrency](https://github.com/gammazero/nexus/wiki/Router-Concurrency) and
  [Client Concurrency](https://github.com/gammazero/nexus/wiki/Client-Concurrency) for details.
- **WAMP Advanced Profile Features**  This project implements most of the advanced profile features in WAMP v2.
  See [current feature support](#advanced-profile-feature-support) provided by nexus.
  Nexus also offers extended functionality for retrieving session information and for message filtering, giving clients
  more ability to decide where to send messages.
- **Flexibility** Multiple transports and serialization options are supported, and more are being developed to maximize
  interoperability. Currently, nexus provides websocket, rawsocket (tcp and unix), and local (in-process) transports.
  [JSON](https://en.wikipedia.org/wiki/JSON), [MessagePack](http://msgpack.org/index.html), and
  [CBOR](https://tools.ietf.org/html/rfc7049) serialization is available over websockets and rawsockets.
- **Security** TLS is available over websockets and rawsockets with client and server APIs that allow configuration
  of TLS. The nexus router library also provides interfaces for integration of client authentication and authorization
  logic.

### Status

The currently maintained version of this module is 3.x. Earlier major versions are no longer supported.

## Advanced Profile Feature Support

### RPC Features

| Feature                      | Supported |
|------------------------------|-----------|
| progressive_call_results     | Yes       |
| progressive_call_invocations | Yes       |
| call_timeout                 | Yes       |
| call_canceling               | Yes       |
| caller_identification        | Yes       |
| call_trustlevels             | No        |
| registration_meta_events     | Yes       |
| registration_meta_procedures | Yes       |
| pattern_based_registration   | Yes       |
| shared_registration          | Yes       |
| force_reregister             | Yes       |
| sharded_registration         | No        |
| registration_revocation      | No [^1]   |
| procedure_reflection         | No        |
| payload_passthru_mode        | Yes       |

### PubSub Features

| Feature                       | Supported |
|-------------------------------|-----------|
| subscriber_blackwhite_listing | Yes       |
| publisher_exclusion           | Yes       |
| publisher_identification      | Yes       |
| publication_trustlevels       | No        |
| subscription_meta_events      | Yes       |
| subscription_meta_procedures  | Yes       |
| pattern_based_subscription    | Yes       |
| sharded_subscription          | No        |
| event_history                 | Yes       |
| topic_reflection              | No        |
| payload_passthru_mode         | Yes       |

### Other Advanced Features

| Feature                           | Supported  |
|-----------------------------------|------------|
| rawsocket (TCP & Unix) transport  | Yes        |
| session meta events               | Yes        |
| session meta procedures           | Yes        |
| testament meta API                | Yes        |
| TLS for websockets                | Yes        |
| TLS for rawsockets                | Yes        |
| challenge-response authentication | Yes        |
| cryptosign authentication         | Yes [^2]   |
| cookie authentication             | Yes        |
| ticket authentication             | Yes        |
| batched WS transport              | No         |
| longpoll transport                | No         |
| websocket compression             | Yes        |

[^1]: The router sends an unsolicited `UNREGISTERED` only when it evicts an old registration because a client
    registered the same procedure with `force_reregister`. There is no API to revoke a registration, and the feature
    is not announced in `WELCOME`.
[^2]: Router side only (`auth.NewCryptoSignAuthenticator`). The nexus client does not implement cryptosign.

## Extended Functionality

Nexus provides [extended functionality](https://github.com/gammazero/nexus/wiki/Extended-Functionality) around
subscriber black/white listing and in the information available via the session meta API. This enhances the
ability of clients to make decisions about message recipients.
