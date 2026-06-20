# Redis Clone in Go

A Redis-compatible server built from scratch in Go, implementing the RESP (REdis Serialization Protocol) wire format, multiple data types, key expiry, and on-disk persistence via an append-only file (AOF).

Built as a learning project to understand how key-value stores, network protocols, and concurrency work under the hood - not a drop-in production replacement for Redis.

## Why I built this

I wanted to actually learn Go by building something real with it, rather than just working through syntax tutorials. Redis felt like a great target: it's something I use as a black box all the time, and reimplementing it from scratch parsing its wire protocol byte by byte, handling concurrent clients safely, and figuring out how persistence and expiry actually work under the hood taught me far more about how databases and network services work than reading about them ever could.

## Features

- **RESP protocol** - custom parser and serializer for simple strings, bulk strings, integers, arrays, errors, and null replies.
- **Concurrent clients** - each connection is handled in its own goroutine; shared data structures are protected with `sync.RWMutex`, verified with Go's race detector under concurrent load.
- **Data types**
  - Strings: `SET`, `GET`, `DEL`, `EXISTS`, `INCR`, `DECR`
  - Hashes: `HSET`, `HGET`, `HGETALL`, `HDEL`, `HEXISTS`, `HLEN`
  - Lists: `LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `LRANGE`, `LLEN`
  - Sets: `SADD`, `SREM`, `SISMEMBER`, `SMEMBERS`, `SCARD`
  - Other: `PING`, `TYPE`, `KEYS` (supports glob patterns like `user:*`), `DBSIZE`
- **Key expiry** - `EXPIRE`, `PEXPIREAT`, `TTL`, lazy expiry on read, and a background goroutine that periodically sweeps and removes expired keys even if they're never looked up again. Expiry is persisted as an absolute timestamp rather than a relative duration, so AOF replay recovers the correct remaining TTL instead of resetting it.
- **Persistence** - every successful write command is appended to `database.aof` in RESP format and replayed on startup, so data survives a restart.
- **Type safety** - Redis-style `WRONGTYPE` errors prevent operations against a key holding an incompatible data type.
- **Authentication** - optional shared-password protection via `AUTH`; unauthenticated clients are rejected until they provide the correct password.
- **Graceful shutdown** - on `SIGINT`/`SIGTERM`, the server stops accepting new connections, waits (with a timeout) for in-flight requests to finish, then flushes and closes the AOF file cleanly before exiting.
- **Configurable** - port, AOF file path, and password are all set via command-line flags rather than hardcoded.
- **Transactions** - `MULTI`/`EXEC`/`DISCARD` with per-connection command queuing. Commands issued inside a transaction are queued and executed atomically on `EXEC`. Nested `MULTI` and unknown commands inside a transaction are rejected immediately.
- **Pub/Sub** - `SUBSCRIBE`, `UNSUBSCRIBE`, `PUBLISH` with per-channel message routing. Subscribers hold open connections and receive messages pushed in real time. Subscriber cleanup on disconnect prevents goroutine leaks.

## Running it

Requires Go installed.

```bash
go run main.go aof.go handler.go resp.go
```

By default this listens on `:6379` (the default Redis port) with no password and persists to `database.aof`. All three are configurable via flags:

```bash
go run main.go aof.go handler.go resp.go -port 6380 -aof mydata.aof -password hunter2
```

Connect with any Redis client, e.g.:

```bash
redis-cli -p 6379
```

If a password is set, authenticate first:

```
127.0.0.1:6379> get foo
(error) NOAUTH Authentication required
127.0.0.1:6379> auth hunter2
OK
127.0.0.1:6379> get foo
(nil)
```

The server shuts down gracefully on `Ctrl+C`: it stops accepting new connections, waits briefly for in-flight requests to finish, then flushes and closes the AOF file before exiting.

## Example session

```
127.0.0.1:6379> set foo bar
OK
127.0.0.1:6379> get foo
"bar"
127.0.0.1:6379> hset user:1 name Alice
(integer) 1
127.0.0.1:6379> hgetall user:1
1) "name"
2) "Alice"
127.0.0.1:6379> rpush queue a b c
(integer) 3
127.0.0.1:6379> lrange queue 0 -1
1) "a"
2) "b"
3) "c"
127.0.0.1:6379> sadd tags go redis
(integer) 2
127.0.0.1:6379> expire foo 30
(integer) 1
127.0.0.1:6379> ttl foo
(integer) 29
```

## Project structure

| File | Responsibility |
|---|---|
| `resp.go` | RESP protocol parsing and serialization (`Value` type, reader, writer) |
| `handler.go` | Command implementations and the command dispatch table |
| `aof.go` | Append-only file persistence (write + replay) |
| `main.go` | TCP server, connection handling, command routing |

## What's intentionally not implemented

This project focuses on the core mechanics of a single-instance key-value store. The following are out of scope:

- Sorted sets, bitmaps, streams, and other advanced data types
- Replication / clustering
- The RDB binary snapshot format
- Lua scripting (`EVAL`)
- Memory eviction policies (`maxmemory`)
- `WATCH`/optimistic locking for transactions — `MULTI`/`EXEC` is supported but keys modified by other clients between `MULTI` and `EXEC` are not detected.

## Known limitations

- Hash, list, and set field/member ordering is not guaranteed (matches Go's map iteration semantics).
- Authentication is a single shared password, not per-user accounts or ACLs.
- `KEYS` scans every key in memory; fine for personal/dev use, not suitable for very large datasets.