package main

import (
	"errors"
	"net"
	"strings"
	"sync"
	"time"

	"miniredis/raft"
	"miniredis/resp"
)

type waiter struct {
	term  int
	reply chan string
}

type node struct {
	raft  *raft.Raft
	ln    net.Listener
	addrs map[string]string

	mu      sync.Mutex
	waiting map[int]waiter
}

func join(s *server, addr, nodes string) (*node, error) {
	n := &node{addrs: map[string]string{}, waiting: map[int]waiter{}}
	peers := map[string]raft.Peer{}
	self := ""

	for _, pair := range strings.Split(nodes, ",") {
		client, cluster, ok := strings.Cut(pair, "=")
		if !ok {
			return nil, errors.New("bad node " + pair + ", want redis=raft")
		}
		n.addrs[cluster] = client
		if client == addr {
			self = cluster
		} else {
			peers[cluster] = raft.Dial(cluster)
		}
	}
	if self == "" {
		return nil, errors.New(addr + " is not in -nodes")
	}

	ln, err := net.Listen("tcp", self)
	if err != nil {
		return nil, err
	}
	n.ln = ln
	n.raft = raft.New(self, peers, func(index, term int, args []string) {
		n.done(index, term, s.exec(args))
	})
	go raft.Listen(n.raft, ln)
	n.raft.Start()
	return n, nil
}

func (n *node) replicate(args []string) string {
	n.mu.Lock()
	index, term, ok := n.raft.Propose(args)
	if !ok {
		n.mu.Unlock()
		if _, _, leader := n.raft.State(); leader != "" {
			return resp.Fail("not the leader, try " + n.addrs[leader])
		}
		return resp.Fail("no leader yet")
	}
	reply := make(chan string, 1)
	n.waiting[index] = waiter{term, reply}
	n.mu.Unlock()

	select {
	case r := <-reply:
		return r
	case <-time.After(2 * time.Second):
		n.mu.Lock()
		delete(n.waiting, index)
		n.mu.Unlock()
		return resp.Fail("timed out waiting for a majority")
	}
}

func (n *node) done(index, term int, reply string) {
	n.mu.Lock()
	w, ok := n.waiting[index]
	delete(n.waiting, index)
	n.mu.Unlock()

	if !ok {
		return
	}
	if w.term != term {
		reply = resp.Fail("lost leadership, retry")
	}
	w.reply <- reply
}
