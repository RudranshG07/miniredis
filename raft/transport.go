package raft

import (
	"errors"
	"net"
	"net/rpc"
	"sync"
	"time"
)

type service struct{ r *Raft }

func (s *service) RequestVote(v Vote, reply *Reply) error {
	*reply = s.r.OnRequestVote(v)
	return nil
}

func (s *service) AppendEntries(a Append, reply *Reply) error {
	*reply = s.r.OnAppendEntries(a)
	return nil
}

func Listen(r *Raft, ln net.Listener) {
	srv := rpc.NewServer()
	srv.RegisterName("Raft", &service{r})
	srv.Accept(ln)
}

type remote struct {
	addr   string
	mu     sync.Mutex
	client *rpc.Client
}

func Dial(addr string) Peer {
	return &remote{addr: addr}
}

func (p *remote) RequestVote(v Vote) (Reply, error) {
	return p.call("Raft.RequestVote", v)
}

func (p *remote) AppendEntries(a Append) (Reply, error) {
	return p.call("Raft.AppendEntries", a)
}

func (p *remote) call(method string, args any) (Reply, error) {
	p.mu.Lock()
	if p.client == nil {
		c, err := rpc.Dial("tcp", p.addr)
		if err != nil {
			p.mu.Unlock()
			return Reply{}, err
		}
		p.client = c
	}
	c := p.client
	p.mu.Unlock()

	reply := new(Reply)
	call := c.Go(method, args, reply, make(chan *rpc.Call, 1))

	select {
	case <-call.Done:
		if call.Error == nil {
			return *reply, nil
		}
		p.drop(c)
		return Reply{}, call.Error
	case <-time.After(timeout):
		p.drop(c)
		return Reply{}, errors.New(p.addr + " timed out")
	}
}

func (p *remote) drop(c *rpc.Client) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.client == c {
		c.Close()
		p.client = nil
	}
}
