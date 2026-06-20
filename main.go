package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

var activeConns sync.WaitGroup

type Client struct {
	multi      bool
	queue      []Value
	subscribed map[string]chan Value
}

func main() {
	port := flag.String("port", "6379", "port to listen on")
	aofPath := flag.String("aof", "database.aof", "AOF file path")
	password := flag.String("password", "", "password to authenticate with. (empty = no auth required)")
	flag.Parse()
	fmt.Println("Listening on port :" + *port)
	l, err := net.Listen("tcp", ":"+*port)
	if err != nil {
		fmt.Println(err)
		return
	}

	aof, err := NewAof(*aofPath)
	if err != nil {
		fmt.Println(err)
		return
	}

	aof.Read(func(v Value) {
		command := strings.ToUpper(v.array[0].bulk)
		args := v.array[1:]

		cmd, ok := Handlers[command]
		if !ok {
			return
		}
		cmd.Handler(args)
	})

	startExpiryReaper()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	shutdownDone := make(chan struct{})

	go func() {
		sig := <-sigCh
		fmt.Println("\nReceived signal:", sig, "- shutting down gracefully...")
		l.Close()
		fmt.Println("Waiting for active connections to finish...")
		waitDone := make(chan struct{})
		go func() {
			activeConns.Wait()
			close(waitDone)
		}()
		select {
		case <-waitDone:
			fmt.Println("All connections closed")
		case <-time.After(5 * time.Second):
			fmt.Println("Timeout waiting for connections to close")
		}
		aof.Close()
		fmt.Println("Shutdown complete")
		close(shutdownDone)
	}()

	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				if errors.Is(err, net.ErrClosed) {
					fmt.Println("Listener closed")
					return
				}
				fmt.Println(err)
				continue
			}
			go handleConnection(conn, aof, *password)
		}
	}()

	<-shutdownDone
}

func handleConnection(conn net.Conn, aof *Aof, password string) {
	defer conn.Close()
	activeConns.Add(1)
	defer activeConns.Done()

	client := &Client{}
	authenticate := password == ""

	defer func() {
		if client.subscribed != nil {
			PubSubMu.Lock()
			for ch, msgCh := range client.subscribed {
				subs := PubSub[ch]
				newSubs := []chan Value{}
				for _, sub := range subs {
					if sub != msgCh {
						newSubs = append(newSubs, sub)
					}
				}
				if len(newSubs) == 0 {
					delete(PubSub, ch)
				} else {
					PubSub[ch] = newSubs
				}
				close(msgCh)
			}
			PubSubMu.Unlock()
		}
	}()

	for {
		resp := NewResp(conn)
		value, err := resp.Read()
		if err != nil {
			if err != io.EOF {
				fmt.Println(err)
			}
			return
		}

		if value.typ != "array" {
			fmt.Println("Invalid request, expected array")
			continue
		}

		if len(value.array) == 0 {
			fmt.Println("Invalid request, expected array length > 0")
			continue
		}

		command := strings.ToUpper(value.array[0].bulk)
		args := value.array[1:]

		writer := NewWriter(conn)

		if command == "AUTH" {
			if len(args) != 1 {
				writer.Write(Value{typ: "error", str: "ERR wrong number of arguments for 'auth' command. Requires atleast one argument"})
				continue
			}
			if args[0].bulk != password {
				writer.Write(Value{typ: "error", str: "NOAUTH invalid password"})
				continue
			}
			authenticate = true
			writer.Write(Value{typ: "string", str: "OK"})
			continue
		}

		if !authenticate {
			writer.Write(Value{typ: "error", str: "NOAUTH Authentication required"})
			continue
		}

		if command == "MULTI" {
			if client.multi {
				writer.Write(Value{typ: "error", str: "ERR MULTI calls can not be nested"})
				continue
			}
			client.multi = true
			client.queue = []Value{}
			writer.Write(Value{typ: "string", str: "OK"})
			continue
		}

		if command == "DISCARD" {
			if !client.multi {
				writer.Write(Value{typ: "error", str: "ERR DISCARD can only be used after MULTI"})
				continue
			}
			client.multi = false
			client.queue = nil
			writer.Write(Value{typ: "string", str: "OK"})
			continue
		}

		if command == "EXEC" {
			if !client.multi {
				writer.Write(Value{typ: "error", str: "ERR EXEC can only be used after MULTI"})
				continue
			}
			client.multi = false
			results := []Value{}
			for _, queued := range client.queue {
				cmd := strings.ToUpper(queued.array[0].bulk)
				args := queued.array[1:]
				handler, ok := Handlers[cmd]
				if !ok {
					results = append(results, Value{typ: "error", str: "ERR unknown command"})
					continue
				}
				result := handler.Handler(args)
				results = append(results, result)
				if handler.IsWrite && result.typ != "error" {
					aof.Write(queued)
				}
			}
			client.queue = nil
			writer.Write(Value{typ: "array", array: results})
			continue
		}

		if client.multi {
			_, ok := Handlers[command]
			if !ok {
				writer.Write(Value{typ: "error", str: "ERR unknown command: " + command})
				continue
			}
			client.queue = append(client.queue, value)
			writer.Write(Value{typ: "string", str: "QUEUED"})
			continue
		}

		if command == "SUBSCRIBE" {
			if len(args) == 0 {
				writer.Write(Value{typ: "error", str: "ERR wrong number of arguments for 'subscribe' command"})
				continue
			}

			if client.subscribed == nil {
				client.subscribed = map[string]chan Value{}
			}

			for _, arg := range args {
				ch := arg.bulk

				if _, already := client.subscribed[ch]; already {
					continue
				}

				msgCh := make(chan Value, 10)
				client.subscribed[ch] = msgCh

				PubSubMu.Lock()
				PubSub[ch] = append(PubSub[ch], msgCh)
				PubSubMu.Unlock()

				writer.Write(Value{typ: "array", array: []Value{
					{typ: "string", str: "subscribe"},
					{typ: "bulk", bulk: ch},
					{typ: "integer", num: len(client.subscribed)},
				}})

				go func(channel string, ch chan Value) {
					for msg := range ch {
						writer.Write(msg)
					}
				}(ch, msgCh)
			}
			continue
		}

		if command == "UNSUBSCRIBE" {
			channels := args
			if len(channels) == 0 {
				for ch := range client.subscribed {
					channels = append(channels, Value{typ: "bulk", bulk: ch})
				}
			}

			for _, arg := range channels {
				ch := arg.bulk
				msgCh, ok := client.subscribed[ch]
				if !ok {
					continue
				}

				PubSubMu.Lock()
				subs := PubSub[ch]
				newSubs := []chan Value{}
				for _, sub := range subs {
					if sub != msgCh {
						newSubs = append(newSubs, sub)
					}
				}
				if len(newSubs) == 0 {
					delete(PubSub, ch)
				} else {
					PubSub[ch] = newSubs
				}
				PubSubMu.Unlock()

				close(msgCh)
				delete(client.subscribed, ch)

				writer.Write(Value{typ: "array", array: []Value{
					{typ: "string", str: "unsubscribe"},
					{typ: "bulk", bulk: ch},
					{typ: "integer", num: len(client.subscribed)},
				}})
			}
			continue
		}

		if command == "PUBLISH" {
			if len(args) != 2 {
				writer.Write(Value{typ: "error", str: "ERR wrong number of arguments for 'publish' command"})
				continue
			}

			ch := args[0].bulk
			message := args[1]

			PubSubMu.RLock()
			subs := PubSub[ch]
			PubSubMu.RUnlock()

			for _, msgCh := range subs {
				msgCh <- Value{typ: "array", array: []Value{
					{typ: "string", str: "message"},
					{typ: "bulk", bulk: ch},
					message,
				}}
			}

			writer.Write(Value{typ: "integer", num: len(subs)})
			continue
		}

		cmd, ok := Handlers[command]
		if !ok {
			fmt.Println("Invalid command: ", command)
			writer.Write(Value{typ: "string", str: ""})
			continue
		}

		result := cmd.Handler(args)

		if cmd.IsWrite && result.typ != "error" {
			if command == "EXPIRE" {
				key := args[0].bulk
				ExpiresMu.RLock()
				expiry, ok := Expires[key]
				ExpiresMu.RUnlock()
				if ok {
					aof.Write(Value{typ: "array", array: []Value{
						{typ: "bulk", bulk: "PEXPIREAT"},
						{typ: "bulk", bulk: key},
						{typ: "bulk", bulk: strconv.FormatInt(expiry.UnixMilli(), 10)},
					}})
				}
			} else {
				aof.Write(value)
			}
		}

		writer.Write(result)
	}
}
