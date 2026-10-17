package main

import (
	"bufio"
	"errors"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
	"time"

	"miniredis/resp"
)

type server struct {
	db   *store
	aof  *aof
	node *node
}

func (s *server) serve(ln net.Listener) {
	for {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		go s.handle(conn)
	}
}

func (s *server) handle(conn net.Conn) {
	defer conn.Close()

	r := bufio.NewReader(conn)
	for {
		args, err := resp.ReadCommand(r)
		if err != nil {
			if err != io.EOF {
				io.WriteString(conn, resp.Fail("protocol error"))
			}
			return
		}
		if _, err := io.WriteString(conn, s.run(args)); err != nil {
			return
		}
	}
}

func (s *server) run(args []string) string {
	if s.node != nil && isWrite(args[0]) {
		return s.node.replicate(args)
	}
	return s.exec(args)
}

func isWrite(cmd string) bool {
	switch strings.ToUpper(cmd) {
	case "SET", "DEL", "FLUSHALL":
		return true
	}
	return false
}

func (s *server) exec(args []string) string {
	cmd := strings.ToUpper(args[0])

	switch {
	case cmd == "PING":
		return resp.Simple("PONG")
	case cmd == "COMMAND":
		return resp.Array(nil)
	case cmd == "KEYS":
		return resp.Array(s.db.keys())
	case cmd == "FLUSHALL":
		s.db.flush()
		s.persist(args)
		return resp.Simple("OK")
	case len(args) < 2:
		return resp.Fail("wrong number of arguments")
	}

	key := args[1]

	switch cmd {
	case "GET":
		v, ok := s.db.get(key)
		if !ok {
			return resp.Nil
		}
		return resp.Bulk(v)

	case "SET":
		if len(args) < 3 {
			return resp.Fail("wrong number of arguments")
		}
		ttl, err := parseTTL(args[3:])
		if err != nil {
			return resp.Fail(err.Error())
		}
		s.db.set(key, args[2], ttl)
		s.persist(args)
		return resp.Simple("OK")

	case "DEL":
		n := s.db.del(args[1:]...)
		s.persist(args)
		return resp.Integer(n)

	case "TTL":
		exp, ok := s.db.expiry(key)
		if !ok {
			return resp.Integer(-2)
		}
		if exp.IsZero() {
			return resp.Integer(-1)
		}
		return resp.Integer(int(time.Until(exp).Round(time.Second).Seconds()))
	}

	return resp.Fail("unknown command '" + args[0] + "'")
}

func (s *server) persist(args []string) {
	if s.aof == nil {
		return
	}
	if err := s.aof.write(args); err != nil {
		log.Printf("aof: %v", err)
	}
}

func parseTTL(opts []string) (time.Duration, error) {
	if len(opts) == 0 {
		return 0, nil
	}
	if len(opts) != 2 {
		return 0, errors.New("syntax error")
	}

	n, err := strconv.Atoi(opts[1])
	if err != nil || n <= 0 {
		return 0, errors.New("invalid expire time")
	}

	switch strings.ToUpper(opts[0]) {
	case "EX":
		return time.Duration(n) * time.Second, nil
	case "PX":
		return time.Duration(n) * time.Millisecond, nil
	}
	return 0, errors.New("syntax error")
}
