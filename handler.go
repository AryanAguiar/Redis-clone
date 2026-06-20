package main

import (
	"path"
	"strconv"
	"sync"
	"time"
)

type Command struct {
	Handler func([]Value) Value
	IsWrite bool
}

var Handlers = map[string]Command{
	"PING":      {Handler: ping, IsWrite: false},
	"GET":       {Handler: get, IsWrite: false},
	"SET":       {Handler: set, IsWrite: true},
	"HSET":      {Handler: hset, IsWrite: true},
	"HGET":      {Handler: hget, IsWrite: false},
	"DEL":       {Handler: del, IsWrite: true},
	"HGETALL":   {Handler: hgetall, IsWrite: false},
	"HDEL":      {Handler: hdel, IsWrite: true},
	"HEXISTS":   {Handler: hexists, IsWrite: false},
	"HLEN":      {Handler: hlen, IsWrite: false},
	"EXISTS":    {Handler: exists, IsWrite: false},
	"EXPIRE":    {Handler: expire, IsWrite: true},
	"TTL":       {Handler: ttl, IsWrite: false},
	"LPUSH":     {Handler: lpush, IsWrite: true},
	"RPUSH":     {Handler: rpush, IsWrite: true},
	"LRANGE":    {Handler: lrange, IsWrite: false},
	"LLEN":      {Handler: llen, IsWrite: false},
	"LPOP":      {Handler: lpop, IsWrite: true},
	"RPOP":      {Handler: rpop, IsWrite: true},
	"SADD":      {Handler: sadd, IsWrite: true},
	"SMEMBERS":  {Handler: smembers, IsWrite: false},
	"SISMEMBER": {Handler: sismember, IsWrite: false},
	"SREM":      {Handler: srem, IsWrite: true},
	"SCARD":     {Handler: scard, IsWrite: false},
	"INCR":      {Handler: incr, IsWrite: true},
	"DECR":      {Handler: decr, IsWrite: true},
	"TYPE":      {Handler: types, IsWrite: false},
	"PEXPIREAT": {Handler: pexpireat, IsWrite: true},
	"KEYS":      {Handler: keys, IsWrite: false},
	"DBSIZE":    {Handler: dbsize, IsWrite: false},
}

func ping(args []Value) Value {
	return Value{typ: "string", str: "Pong"}
}

// data structures
var SETs = map[string]string{}
var SETsMu = sync.RWMutex{}
var HSETs = map[string]map[string]string{}
var HSETsMu = sync.RWMutex{}
var Expires = map[string]time.Time{}
var ExpiresMu = sync.RWMutex{}
var Lists = map[string][]string{}
var ListsMu = sync.RWMutex{}
var Sets = map[string]map[string]struct{}{} //Sets and SetsMu are for set data structure
var SetsMu = sync.RWMutex{}
var PubSub = map[string][]chan Value{}
var PubSubMu = sync.RWMutex{}

// helper
func isExpired(key string) bool {
	ExpiresMu.RLock()
	expiry, hasExpiry := Expires[key]
	ExpiresMu.RUnlock()

	if !hasExpiry {
		return false
	}

	if time.Now().After(expiry) {
		SETsMu.Lock()
		delete(SETs, key)
		SETsMu.Unlock()

		HSETsMu.Lock()
		delete(HSETs, key)
		HSETsMu.Unlock()

		ExpiresMu.Lock()
		delete(Expires, key)
		ExpiresMu.Unlock()

		ListsMu.Lock()
		delete(Lists, key)
		ListsMu.Unlock()

		SetsMu.Lock()
		delete(Sets, key)
		SetsMu.Unlock()
		return true
	}
	return false
}

func startExpiryReaper() {
	go func() {
		for {
			time.Sleep(1 * time.Second)
			ExpiresMu.RLock()
			now := time.Now()
			var expiredKeys []string
			for key, expiry := range Expires {
				if now.After(expiry) {
					expiredKeys = append(expiredKeys, key)
				}
			}
			ExpiresMu.RUnlock()

			for _, key := range expiredKeys {
				isExpired(key)
			}
		}
	}()
}

func wrongType(key, expectedType string) bool {
	switch expectedType {
	case "string":
		HSETsMu.RLock()
		_, inHSETs := HSETs[key]
		HSETsMu.RUnlock()
		if inHSETs {
			return true
		}

		ListsMu.RLock()
		_, inLists := Lists[key]
		ListsMu.RUnlock()
		if inLists {
			return true
		}

		SetsMu.RLock()
		_, inSets := Sets[key]
		SetsMu.RUnlock()
		if inSets {
			return true
		}

	case "hash":
		SETsMu.RLock()
		_, inSETs := SETs[key]
		SETsMu.RUnlock()
		if inSETs {
			return true
		}

		ListsMu.RLock()
		_, inLists := Lists[key]
		ListsMu.RUnlock()
		if inLists {
			return true
		}

		SetsMu.RLock()
		_, inSets := Sets[key]
		SetsMu.RUnlock()
		if inSets {
			return true
		}

	case "list":
		SETsMu.RLock()
		_, inSETs := SETs[key]
		SETsMu.RUnlock()
		if inSETs {
			return true
		}

		HSETsMu.RLock()
		_, inHSETs := HSETs[key]
		HSETsMu.RUnlock()
		if inHSETs {
			return true
		}

		SetsMu.RLock()
		_, inSets := Sets[key]
		SetsMu.RUnlock()
		if inSets {
			return true
		}
	case "set":
		SETsMu.RLock()
		_, inSETs := SETs[key]
		SETsMu.RUnlock()
		if inSETs {
			return true
		}

		HSETsMu.RLock()
		_, inHSETs := HSETs[key]
		HSETsMu.RUnlock()
		if inHSETs {
			return true
		}

		ListsMu.RLock()
		_, inLists := Lists[key]
		ListsMu.RUnlock()
		if inLists {
			return true
		}
	}
	return false
}

// functions
func set(args []Value) Value {
	if len(args) != 2 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'set' command, expected 2"}
	}

	if wrongType(args[0].bulk, "string") {
		return Value{typ: "error", str: "WRONGTYPE Operation against a key holding the wrong kind of value"}
	}

	key := args[0].bulk
	value := args[1].bulk

	isExpired(key)

	SETsMu.Lock()
	SETs[key] = value
	SETsMu.Unlock()

	return Value{typ: "string", str: "OK"}
}

func get(args []Value) Value {
	if len(args) != 1 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'get' command, expected 1"}
	}

	if wrongType(args[0].bulk, "string") {
		return Value{typ: "error", str: "WRONGTYPE Operation against a key holding the wrong kind of value"}
	}

	key := args[0].bulk

	if isExpired(key) {
		return Value{typ: "null"}
	}

	SETsMu.RLock()
	val, ok := SETs[key]
	SETsMu.RUnlock()

	if !ok {
		return Value{typ: "null"}
	}

	return Value{typ: "bulk", bulk: val}
}

func hset(args []Value) Value {
	if len(args) != 3 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'hset' command, expected 3"}
	}

	if wrongType(args[0].bulk, "hash") {
		return Value{typ: "error", str: "WRONGTYPE Operation against a key holding the wrong kind of value"}
	}

	hash := args[0].bulk
	key := args[1].bulk
	value := args[2].bulk

	isExpired(hash)

	HSETsMu.Lock()
	if _, ok := HSETs[hash]; !ok {
		HSETs[hash] = map[string]string{}
	}

	_, existed := HSETs[hash][key]
	HSETs[hash][key] = value
	HSETsMu.Unlock()

	if existed {
		return Value{typ: "integer", num: 0}
	}
	return Value{typ: "integer", num: 1}
}

func hget(args []Value) Value {
	if len(args) != 2 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'hget' command, expected 2"}
	}

	if wrongType(args[0].bulk, "hash") {
		return Value{typ: "error", str: "WRONGTYPE Operation against a key holding the wrong kind of value"}
	}

	hash := args[0].bulk
	key := args[1].bulk

	if isExpired(hash) {
		return Value{typ: "null"}
	}

	HSETsMu.RLock()
	value, ok := HSETs[hash][key]
	HSETsMu.RUnlock()

	if !ok {
		return Value{typ: "null"}
	}

	return Value{typ: "bulk", bulk: value}
}

func hgetall(args []Value) Value {
	if len(args) != 1 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'hgetall' command, expected 1"}
	}

	if wrongType(args[0].bulk, "hash") {
		return Value{typ: "error", str: "WRONGTYPE Operation against a key holding the wrong kind of value"}
	}

	hash := args[0].bulk

	HSETsMu.RLock()
	hashMap, ok := HSETs[hash]
	HSETsMu.RUnlock()

	if !ok {
		return Value{typ: "array", array: []Value{}}
	}

	if isExpired(hash) {
		return Value{typ: "array", array: []Value{}}
	}

	result := []Value{}
	for key, value := range hashMap {
		result = append(result, Value{typ: "bulk", bulk: key})
		result = append(result, Value{typ: "bulk", bulk: value})
	}

	return Value{typ: "array", array: result}
}

func hdel(args []Value) Value {
	if len(args) != 2 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'hdel' command, expected 2"}
	}

	if wrongType(args[0].bulk, "hash") {
		return Value{typ: "error", str: "WRONGTYPE Operation against a key holding the wrong kind of value"}
	}

	hash := args[0].bulk
	key := args[1].bulk

	if isExpired(hash) {
		return Value{typ: "integer", num: 0}
	}

	HSETsMu.Lock()
	defer HSETsMu.Unlock()

	_, existed := HSETs[hash][key]
	if !existed {
		return Value{typ: "integer", num: 0}
	}

	delete(HSETs[hash], key)
	return Value{typ: "integer", num: 1}
}

func hexists(args []Value) Value {
	if len(args) != 2 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'hexists' command, expected 2"}
	}

	if wrongType(args[0].bulk, "hash") {
		return Value{typ: "error", str: "WRONGTYPE Operation against a key holding the wrong kind of value"}
	}

	hash := args[0].bulk
	key := args[1].bulk

	if isExpired(hash) {
		return Value{typ: "integer", num: 0}
	}

	HSETsMu.RLock()
	defer HSETsMu.RUnlock()

	_, ok := HSETs[hash][key]
	if !ok {
		return Value{typ: "integer", num: 0}
	}
	return Value{typ: "integer", num: 1}
}

func hlen(args []Value) Value {
	if len(args) != 1 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'hlen' command, expected 1"}
	}

	if wrongType(args[0].bulk, "hash") {
		return Value{typ: "error", str: "WRONGTYPE Operation against a key holding the wrong kind of value"}
	}

	hash := args[0].bulk

	if isExpired(hash) {
		return Value{typ: "integer", num: 0}
	}

	HSETsMu.RLock()
	defer HSETsMu.RUnlock()

	return Value{typ: "integer", num: len(HSETs[hash])}
}

func del(args []Value) Value {
	if len(args) != 1 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'del' command, expected 1"}
	}

	key := args[0].bulk

	SETsMu.Lock()
	_, ok := SETs[key]
	delete(SETs, key)
	SETsMu.Unlock()

	HSETsMu.Lock()
	_, okHash := HSETs[key]
	delete(HSETs, key)
	HSETsMu.Unlock()

	ListsMu.Lock()
	_, okList := Lists[key]
	delete(Lists, key)
	ListsMu.Unlock()

	SetsMu.Lock()
	_, okSetType := Sets[key]
	delete(Sets, key)
	SetsMu.Unlock()

	if !ok && !okHash && !okList && !okSetType {
		return Value{typ: "integer", num: 0}
	}

	return Value{typ: "integer", num: 1}
}

func exists(args []Value) Value {
	if len(args) != 1 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'exists' command, expected 1"}
	}

	key := args[0].bulk

	SETsMu.RLock()
	_, ok := SETs[key]
	SETsMu.RUnlock()

	HSETsMu.RLock()
	_, okHash := HSETs[key]
	HSETsMu.RUnlock()

	ListsMu.RLock()
	_, okList := Lists[key]
	ListsMu.RUnlock()

	SetsMu.RLock()
	_, okSetType := Sets[key]
	SetsMu.RUnlock()

	if !ok && !okHash && !okList && !okSetType {
		return Value{typ: "integer", num: 0}
	}

	if isExpired(key) {
		return Value{typ: "integer", num: 0}
	}

	return Value{typ: "integer", num: 1}
}

func expire(args []Value) Value {
	if len(args) != 2 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'expire' command, expected 2"}
	}

	key := args[0].bulk
	ttl, err := strconv.Atoi(args[1].bulk)
	if err != nil {
		return Value{typ: "error", str: "ERR invalid ttl"}
	}

	SETsMu.RLock()
	_, ok := SETs[key]
	SETsMu.RUnlock()

	HSETsMu.RLock()
	_, okHash := HSETs[key]
	HSETsMu.RUnlock()

	ListsMu.RLock()
	_, okList := Lists[key]
	ListsMu.RUnlock()

	SetsMu.RLock()
	_, okSetType := Sets[key]
	SetsMu.RUnlock()

	if !ok && !okHash && !okList && !okSetType {
		return Value{typ: "integer", num: 0}
	}

	ExpiresMu.Lock()
	Expires[key] = time.Now().Add(time.Duration(ttl) * time.Second)
	ExpiresMu.Unlock()

	return Value{typ: "integer", num: 1}
}

func ttl(args []Value) Value {
	if len(args) != 1 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'ttl' command, expected 1"}
	}

	key := args[0].bulk

	SETsMu.RLock()
	_, ok := SETs[key]
	SETsMu.RUnlock()

	HSETsMu.RLock()
	_, okHash := HSETs[key]
	HSETsMu.RUnlock()

	ListsMu.RLock()
	_, okList := Lists[key]
	ListsMu.RUnlock()

	SetsMu.RLock()
	_, okSetType := Sets[key]
	SetsMu.RUnlock()

	if !ok && !okHash && !okList && !okSetType {
		return Value{typ: "integer", num: -2}
	}

	if isExpired(key) {
		return Value{typ: "integer", num: -2}
	}

	ExpiresMu.RLock()
	expiry, hasExpiry := Expires[key]
	ExpiresMu.RUnlock()

	if !hasExpiry {
		return Value{typ: "integer", num: -1}
	}

	remaining := int(time.Until(expiry).Seconds())
	if remaining < 0 {
		return Value{typ: "integer", num: -1}
	}
	return Value{typ: "integer", num: remaining}
}

func rpush(args []Value) Value {
	if len(args) != 2 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'rpush' command, expected 2"}
	}

	if wrongType(args[0].bulk, "list") {
		return Value{typ: "error", str: "WRONGTYPE Operation against a key holding the wrong kind of value"}
	}

	key := args[0].bulk
	value := args[1].bulk

	isExpired(key)

	ListsMu.Lock()
	Lists[key] = append(Lists[key], value)
	length := len(Lists[key])
	ListsMu.Unlock()

	return Value{typ: "integer", num: length}
}

func lpush(args []Value) Value {
	if len(args) != 2 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'lpush' command, expected 2"}
	}

	if wrongType(args[0].bulk, "list") {
		return Value{typ: "error", str: "WRONGTYPE Operation against a key holding the wrong kind of value"}
	}

	key := args[0].bulk
	value := args[1].bulk

	isExpired(key)

	ListsMu.Lock()
	Lists[key] = append([]string{value}, Lists[key]...)
	length := len(Lists[key])
	ListsMu.Unlock()

	return Value{typ: "integer", num: length}
}

func lrange(args []Value) Value {
	if len(args) != 3 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'lrange' command, expected 3"}
	}

	if wrongType(args[0].bulk, "list") {
		return Value{typ: "error", str: "WRONGTYPE Operation against a key holding the wrong kind of value"}
	}

	key := args[0].bulk
	start, err1 := strconv.Atoi(args[1].bulk)
	stop, err2 := strconv.Atoi(args[2].bulk)
	if err1 != nil || err2 != nil {
		return Value{typ: "error", str: "ERR value not an integer or invalid range"}
	}

	ListsMu.RLock()
	list, ok := Lists[key]
	ListsMu.RUnlock()

	n := len(list)

	if !ok {
		return Value{typ: "array", array: []Value{}}
	}

	if start < 0 {
		start = n + start
	}

	if stop < 0 {
		stop = n + stop
	}

	if start < 0 {
		start = 0
	}

	if stop >= n {
		stop = n - 1
	}

	if start > stop || n == 0 {
		return Value{typ: "array", array: []Value{}}
	}

	result := []Value{}
	for i := start; i <= stop; i++ {
		result = append(result, Value{typ: "bulk", bulk: list[i]})
	}

	return Value{typ: "array", array: result}
}

func llen(args []Value) Value {
	if len(args) != 1 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'llen' command, expected 1"}
	}

	if wrongType(args[0].bulk, "list") {
		return Value{typ: "error", str: "WRONGTYPE Operation against a key holding the wrong kind of value"}
	}

	key := args[0].bulk

	if isExpired(key) {
		return Value{typ: "integer", num: 0}
	}

	ListsMu.RLock()
	list, _ := Lists[key]
	ListsMu.RUnlock()

	return Value{typ: "integer", num: len(list)}
}

func lpop(args []Value) Value {
	if len(args) != 1 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'lpop' command, expected 1"}
	}

	if wrongType(args[0].bulk, "list") {
		return Value{typ: "error", str: "WRONGTYPE Operation against a key holding the wrong kind of value"}
	}

	key := args[0].bulk

	if isExpired(key) {
		return Value{typ: "null"}
	}

	ListsMu.Lock()
	defer ListsMu.Unlock()

	list, ok := Lists[key]
	if !ok || len(list) == 0 {
		return Value{typ: "null"}
	}

	value := list[0]

	newList := make([]string, len(list)-1)
	copy(newList, list[1:])
	Lists[key] = newList

	return Value{typ: "bulk", bulk: value}
}

func rpop(args []Value) Value {
	if len(args) != 1 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'rpop' command, expected 1"}
	}

	if wrongType(args[0].bulk, "list") {
		return Value{typ: "error", str: "WRONGTYPE Operation against a key holding the wrong kind of value"}
	}

	key := args[0].bulk

	if isExpired(key) {
		return Value{typ: "null"}
	}

	ListsMu.Lock()
	defer ListsMu.Unlock()

	list, ok := Lists[key]
	if !ok || len(list) == 0 {
		return Value{typ: "null"}
	}

	value := list[len(list)-1]
	Lists[key] = list[:len(list)-1]

	return Value{typ: "bulk", bulk: value}
}

func sadd(args []Value) Value {
	if len(args) != 2 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'sadd' command, expected 2"}
	}

	if wrongType(args[0].bulk, "set") {
		return Value{typ: "error", str: "WRONGTYPE Operation against a key holding the wrong kind of value"}
	}

	key := args[0].bulk
	value := args[1].bulk

	isExpired(key)

	SetsMu.Lock()
	defer SetsMu.Unlock()

	if _, ok := Sets[key]; !ok {
		Sets[key] = map[string]struct{}{}
	}

	_, existed := Sets[key][value]
	Sets[key][value] = struct{}{}

	if existed {
		return Value{typ: "integer", num: 0}
	}
	return Value{typ: "integer", num: 1}
}

func sismember(args []Value) Value {
	if len(args) != 2 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'sismember' command, expected 2"}
	}

	if wrongType(args[0].bulk, "set") {
		return Value{typ: "error", str: "WRONGTYPE Operation against a key holding the wrong kind of value"}
	}

	key := args[0].bulk
	value := args[1].bulk

	if isExpired(key) {
		return Value{typ: "integer", num: 0}
	}

	SetsMu.RLock()
	_, exists := Sets[key][value]
	SetsMu.RUnlock()

	if !exists {
		return Value{typ: "integer", num: 0}
	}

	return Value{typ: "integer", num: 1}
}

func srem(args []Value) Value {
	if len(args) != 2 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'srem' command, expected 2"}
	}

	if wrongType(args[0].bulk, "set") {
		return Value{typ: "error", str: "WRONGTYPE Operation against a key holding the wrong kind of value"}
	}

	key := args[0].bulk
	value := args[1].bulk

	if isExpired(key) {
		return Value{typ: "integer", num: 0}
	}

	SetsMu.Lock()
	defer SetsMu.Unlock()

	_, exists := Sets[key][value]
	delete(Sets[key], value)

	if !exists {
		return Value{typ: "integer", num: 0}
	}
	return Value{typ: "integer", num: 1}
}

func smembers(args []Value) Value {
	if len(args) != 1 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'smembers' command, expected 1"}
	}

	if wrongType(args[0].bulk, "set") {
		return Value{typ: "error", str: "WRONGTYPE Operation against a key holding the wrong kind of value"}
	}

	key := args[0].bulk

	if isExpired(key) {
		return Value{typ: "array", array: []Value{}}
	}

	SetsMu.RLock()
	set := Sets[key]
	SetsMu.RUnlock()

	result := []Value{}
	for key := range set {
		result = append(result, Value{typ: "bulk", bulk: key})
	}

	return Value{typ: "array", array: result}
}

func scard(args []Value) Value {
	if len(args) != 1 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'scard' command, expected 1"}
	}

	if wrongType(args[0].bulk, "set") {
		return Value{typ: "error", str: "WRONGTYPE Operation against a key holding the wrong kind of value"}
	}

	key := args[0].bulk

	if isExpired(key) {
		return Value{typ: "integer", num: 0}
	}

	SetsMu.RLock()
	defer SetsMu.RUnlock()

	set := Sets[key]
	return Value{typ: "integer", num: len(set)}
}

func incr(args []Value) Value {
	if len(args) != 1 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'incr' command, expected 1"}
	}

	if wrongType(args[0].bulk, "string") {
		return Value{typ: "error", str: "WRONGTYPE Operation against a key holding the wrong kind of value"}
	}

	key := args[0].bulk

	isExpired(key)

	SETsMu.Lock()
	defer SETsMu.Unlock()

	current, ok := SETs[key]
	if !ok {
		current = "0"
	}

	num, err := strconv.Atoi(current)
	if err != nil {
		return Value{typ: "error", str: "ERR value is not an integer or out of range"}
	}

	num++
	SETs[key] = strconv.Itoa(num)

	return Value{typ: "integer", num: num}
}

func decr(args []Value) Value {
	if len(args) != 1 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'decr' command, expected 1"}
	}

	if wrongType(args[0].bulk, "string") {
		return Value{typ: "error", str: "WRONGTYPE Operation against a key holding the wrong kind of value"}
	}

	key := args[0].bulk

	isExpired(key)

	SETsMu.Lock()
	defer SETsMu.Unlock()

	current, ok := SETs[key]
	if !ok {
		current = "0"
	}

	num, err := strconv.Atoi(current)
	if err != nil {
		return Value{typ: "error", str: "ERR value is not an integer or out of range"}
	}

	num--
	SETs[key] = strconv.Itoa(num)

	return Value{typ: "integer", num: num}
}

func types(args []Value) Value {
	if len(args) != 1 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'type' command, expected 1"}
	}

	key := args[0].bulk

	if isExpired(key) {
		return Value{typ: "string", str: "none"}
	}

	SETsMu.RLock()
	_, isString := SETs[key]
	SETsMu.RUnlock()

	if isString {
		return Value{typ: "string", str: "string"}
	}

	HSETsMu.RLock()
	_, isHash := HSETs[key]
	HSETsMu.RUnlock()

	if isHash {
		return Value{typ: "string", str: "hash"}
	}

	ListsMu.RLock()
	_, isList := Lists[key]
	ListsMu.RUnlock()

	if isList {
		return Value{typ: "string", str: "list"}
	}

	SetsMu.RLock()
	_, isSet := Sets[key]
	SetsMu.RUnlock()

	if isSet {
		return Value{typ: "string", str: "set"}
	}

	return Value{typ: "string", str: "none"}
}

func pexpireat(args []Value) Value {
	if len(args) != 2 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'pexpireat' command, expected 2"}
	}

	key := args[0].bulk
	unixMills, err := strconv.ParseInt(args[1].bulk, 10, 64)
	if err != nil {
		return Value{typ: "error", str: "ERR value is not an integer or out of range"}
	}

	SETsMu.RLock()
	_, okSet := SETs[key]
	SETsMu.RUnlock()

	HSETsMu.RLock()
	_, okHash := HSETs[key]
	HSETsMu.RUnlock()

	ListsMu.RLock()
	_, okList := Lists[key]
	ListsMu.RUnlock()

	SetsMu.RLock()
	_, okSetType := Sets[key]
	SetsMu.RUnlock()

	if !okSet && !okHash && !okList && !okSetType {
		return Value{typ: "integer", num: 0}
	}

	ExpiresMu.Lock()
	Expires[key] = time.UnixMilli(unixMills)
	ExpiresMu.Unlock()

	return Value{typ: "integer", num: 1}
}

func keys(args []Value) Value {
	if len(args) != 1 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'keys' command, expected 1"}
	}

	pattern := args[0].bulk
	seen := map[string]struct{}{}
	result := []Value{}

	collect := func(key string) {
		if _, already := seen[key]; already {
			return
		}
		seen[key] = struct{}{}
		if isExpired(key) {
			return
		}
		matched, err := path.Match(pattern, key)
		if err != nil || !matched {
			return
		}
		result = append(result, Value{typ: "bulk", bulk: key})
	}

	SETsMu.RLock()
	setKeys := make([]string, 0, len(SETs))
	for k := range SETs {
		setKeys = append(setKeys, k)
	}
	SETsMu.RUnlock()
	for _, k := range setKeys {
		collect(k)
	}

	HSETsMu.RLock()
	hsetKeys := make([]string, 0, len(HSETs))
	for k := range HSETs {
		hsetKeys = append(hsetKeys, k)
	}
	HSETsMu.RUnlock()
	for _, k := range hsetKeys {
		collect(k)
	}

	ListsMu.RLock()
	listKeys := make([]string, 0, len(Lists))
	for k := range Lists {
		listKeys = append(listKeys, k)
	}
	ListsMu.RUnlock()
	for _, k := range listKeys {
		collect(k)
	}

	SetsMu.RLock()
	setKeys = make([]string, 0, len(Sets))
	for k := range Sets {
		setKeys = append(setKeys, k)
	}
	SetsMu.RUnlock()
	for _, k := range setKeys {
		collect(k)
	}

	return Value{typ: "array", array: result}
}

func dbsize(args []Value) Value {
	if len(args) >= 1 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'dbsize' command, expected 0"}
	}

	SETsMu.RLock()
	s := len(SETs)
	SETsMu.RUnlock()

	HSETsMu.RLock()
	h := len(HSETs)
	HSETsMu.RUnlock()

	ListsMu.RLock()
	l := len(Lists)
	ListsMu.RUnlock()

	SetsMu.RLock()
	st := len(Sets)
	SetsMu.RUnlock()

	return Value{typ: "integer", num: s + h + l + st}
}
