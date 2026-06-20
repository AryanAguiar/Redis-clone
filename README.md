# Redis Clone in Go

A Redis-compatible server built from scratch in Go, implementing the RESP (REdis Serialization Protocol) wire format, multiple data types, key expiry, and on-disk persistence via an append-only file (AOF).

Built as a learning project to understand how key-value stores, network protocols, and concurrency work under the hood. It is not intended to be a production replacement for Redis.

## Why I built this

To better understand Redis internals, I built a Redis clone in Go. The project involved implementing the RESP protocol, handling concurrent client connections, managing key expiration, and building append-only file persistence with replay on startup. It provided hands-on experience with database architecture, networking, persistence, and concurrent systems design.

## Features

- **RESP protocol** - custom parser and serializer for simple strings, bulk strings, integers, arrays, errors, and null replies.
- **Concurrent clients** - each connection is handled in its own goroutine; shared data structures are protected with `sync.RWMutex`.
- **Data types**
  - Strings: `SET`, `GET`, `DEL`, `EXISTS`, `INCR`, `DECR`
  - Hashes: `HSET`, `HGET`, `HGETALL`, `HDEL`, `HEXISTS`, `HLEN`
  - Lists: `LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `LRANGE`, `LLEN`
  - Sets: `SADD`, `SREM`, `SISMEMBER`, `SMEMBERS`, `SCARD`
  - Other: `PING`, `TYPE`, `KEYS` (supports glob patterns like `user:*`), DBSIZE`
- **Key expiry** - `EXPIRE`, `PEXPIREAT`, `TTL`, lazy expiry on read, and a background goroutine that periodically sweeps and removes expired keys even if they're never looked up again. Expiry is persisted as an absolute timestamp (`PEXPIREAT`) rather than a relative duration, so AOF replay recovers the correct remaining TTL rather than resetting it.
- **Persistence** - every successful write command is appended to `database.aof` in RESP format and replayed on startup, so data survives a restart.
- **Type safety** - Redis-style `WRONGTYPE` errors prevent operations against incompatible data structures.

## Running it

Requires Go installed.

```bash
go run *.go
```

The server listens on `:6379`, the default Redis port. Connect with any Redis client, e.g.:

```bash
redis-cli -p 6379
```

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
- Transactions (`MULTI`/`EXEC`)
- Pub/Sub
- Memory eviction policies (`maxmemory`)

## Known limitations
- Hash, list, and set field/member ordering is not guaranteed (matches Go's map iteration semantics).