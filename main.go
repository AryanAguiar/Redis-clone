package main

import (
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
)

func main() {
	fmt.Println("Listening on port :6379")

	l, err := net.Listen("tcp", ":6379")
	if err != nil {
		fmt.Println(err)
		return
	}

	aof, err := NewAof("database.aof")
	if err != nil {
		fmt.Println(err)
		return
	}

	defer aof.Close()

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

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println(err)
			continue
		}
		go handleConnection(conn, aof)
	}

}

func handleConnection(conn net.Conn, aof *Aof) {
	defer conn.Close()

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
