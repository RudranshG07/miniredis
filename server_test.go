package main

import (
	"bufio"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"miniredis/resp"
)

func TestCommands(t *testing.T) {
	s := &server{db: newStore()}

	steps := []struct {
		args []string
		want string
	}{
		{[]string{"SET", "a", "1"}, "+OK\r\n"},
		{[]string{"GET", "a"}, "$1\r\n1\r\n"},
		{[]string{"GET", "b"}, resp.Nil},
		{[]string{"DEL", "a", "b"}, ":1\r\n"},
		{[]string{"GET", "a"}, resp.Nil},
		{[]string{"SET", "a", "1", "EX", "10"}, "+OK\r\n"},
		{[]string{"TTL", "a"}, ":10\r\n"},
		{[]string{"TTL", "b"}, ":-2\r\n"},
	}

	for _, step := range steps {
		if got := s.run(step.args); got != step.want {
			t.Fatalf("%v = %q, want %q", step.args, got, step.want)
		}
	}
}

func TestExpiry(t *testing.T) {
	s := &server{db: newStore()}
	s.run([]string{"SET", "a", "1", "PX", "30"})

	time.Sleep(60 * time.Millisecond)

	if got := s.run([]string{"GET", "a"}); got != resp.Nil {
		t.Fatalf("GET after expiry = %q", got)
	}
}

func TestPipelined(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go (&server{db: newStore()}).serve(ln)

	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	conn.Write([]byte(resp.Array([]string{"SET", "a", "1"}) + resp.Array([]string{"GET", "a"})))

	r := bufio.NewReader(conn)
	first, _ := resp.ReadReply(r)
	second, _ := resp.ReadReply(r)
	if first != "+OK\r\n" || second != "$1\r\n1\r\n" {
		t.Fatalf("got %q then %q", first, second)
	}
}

func TestReplay(t *testing.T) {
	path := filepath.Join(t.TempDir(), "log")

	a, _, err := openAOF(path, true)
	if err != nil {
		t.Fatal(err)
	}
	s := &server{db: newStore(), aof: a}
	s.run([]string{"SET", "a", "1"})
	s.run([]string{"SET", "b", "2"})
	s.run([]string{"GET", "a"})
	s.run([]string{"DEL", "b"})
	a.f.Close()

	_, cmds, err := openAOF(path, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(cmds) != 3 {
		t.Fatalf("logged %d commands, want 3, reads should not be logged", len(cmds))
	}

	fresh := &server{db: newStore()}
	for _, args := range cmds {
		fresh.run(args)
	}
	if v, _ := fresh.db.get("a"); v != "1" {
		t.Fatalf("a = %q after replay", v)
	}
	if _, ok := fresh.db.get("b"); ok {
		t.Fatal("b came back after a replayed DEL")
	}
}

func TestTruncatedTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "log")
	torn := "*3\r\n$3\r\nSET\r\n$1\r\nb"
	if err := os.WriteFile(path, []byte(resp.Array([]string{"SET", "a", "1"})+torn), 0o644); err != nil {
		t.Fatal(err)
	}

	a, cmds, err := openAOF(path, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(cmds) != 1 {
		t.Fatalf("got %v, want only the complete command", cmds)
	}
	a.write([]string{"SET", "after", "restart"})
	a.f.Close()

	_, cmds, _ = openAOF(path, true)
	if len(cmds) != 2 || cmds[1][1] != "after" {
		t.Fatalf("got %v, the write after the restart was lost", cmds)
	}
}

func TestCluster(t *testing.T) {
	var addrs []string
	for i := 0; i < 6; i++ {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		addrs = append(addrs, ln.Addr().String())
		ln.Close()
	}
	spec := addrs[0] + "=" + addrs[3] + "," + addrs[1] + "=" + addrs[4] + "," + addrs[2] + "=" + addrs[5]

	var servers []*server
	for _, addr := range addrs[:3] {
		s := &server{db: newStore()}
		n, err := join(s, addr, spec)
		if err != nil {
			t.Fatal(err)
		}
		s.node = n
		t.Cleanup(func() {
			n.ln.Close()
			n.raft.Stop()
		})
		servers = append(servers, s)
	}

	var lead *server
	for start := time.Now(); lead == nil && time.Since(start) < 5*time.Second; time.Sleep(20 * time.Millisecond) {
		for _, s := range servers {
			if role, _, _ := s.node.raft.State(); role == "leader" {
				lead = s
			}
		}
	}
	if lead == nil {
		t.Fatal("no leader")
	}

	if got := lead.run([]string{"SET", "user", "rudransh"}); got != "+OK\r\n" {
		t.Fatalf("SET on leader = %q", got)
	}

	for _, s := range servers {
		if s == lead {
			continue
		}
		if got := s.run([]string{"SET", "x", "1"}); !strings.Contains(got, "not the leader") {
			t.Fatalf("SET on follower = %q", got)
		}

		for start := time.Now(); ; time.Sleep(20 * time.Millisecond) {
			if v, _ := s.db.get("user"); v == "rudransh" {
				break
			}
			if time.Since(start) > 3*time.Second {
				t.Fatal("follower never applied the write")
			}
		}
	}
}
