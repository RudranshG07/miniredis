package raft

import (
	"math/rand"
	"sync"
	"time"
)

const (
	follower  = "follower"
	candidate = "candidate"
	leader    = "leader"

	heartbeat = 50 * time.Millisecond
	timeout   = 300 * time.Millisecond
)

type Entry struct {
	Term int
	Cmd  []string
}

type Vote struct {
	Term      int
	Candidate string
	LastIdx   int
	LastTerm  int
}

type Append struct {
	Term     int
	Leader   string
	PrevIdx  int
	PrevTerm int
	Entries  []Entry
	Commit   int
}

type Reply struct {
	Term int
	OK   bool
	Hint int
}

type Peer interface {
	RequestVote(Vote) (Reply, error)
	AppendEntries(Append) (Reply, error)
}

type Raft struct {
	mu    sync.Mutex
	id    string
	peers map[string]Peer
	apply func(index, term int, cmd []string)

	term     int
	votedFor string
	log      []Entry
	commit   int
	applied  int

	role   string
	leader string
	next   map[string]int
	match  map[string]int

	heard time.Time
	acked time.Time
	wait  time.Duration

	kick chan struct{}
	sent time.Time
	stop chan struct{}
	done sync.WaitGroup
}

func New(id string, peers map[string]Peer, apply func(index, term int, cmd []string)) *Raft {
	if apply == nil {
		apply = func(int, int, []string) {}
	}
	r := &Raft{
		id:    id,
		peers: peers,
		apply: apply,
		log:   []Entry{{}},
		role:  follower,
		next:  map[string]int{},
		match: map[string]int{},
		kick:  make(chan struct{}, 1),
		stop:  make(chan struct{}),
	}
	r.reset()
	return r
}

func (r *Raft) Start() {
	r.done.Add(1)
	go r.loop()
}

func (r *Raft) Stop() {
	close(r.stop)
	r.done.Wait()
}

func (r *Raft) State() (role string, term int, leader string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.role, r.term, r.leader
}

func (r *Raft) Propose(cmd []string) (int, int, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.role != leader {
		return 0, 0, false
	}
	r.log = append(r.log, Entry{Term: r.term, Cmd: cmd})
	select {
	case r.kick <- struct{}{}:
	default:
	}
	return len(r.log) - 1, r.term, true
}

func (r *Raft) loop() {
	defer r.done.Done()

	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()

	for {
		kicked := false
		select {
		case <-r.stop:
			return
		case <-tick.C:
		case <-r.kick:
			kicked = true
		}

		r.mu.Lock()
		role := r.role
		due := kicked || time.Since(r.sent) >= heartbeat
		lost := role == leader && time.Since(r.acked) > r.wait
		expired := role != leader && time.Since(r.heard) > r.wait
		if lost {
			r.stepDown(r.term)
		}
		r.mu.Unlock()

		switch {
		case role == leader && !lost && due:
			r.replicate()
		case expired:
			r.campaign()
		}
		r.flush()
	}
}

func (r *Raft) reset() {
	r.heard = time.Now()
	r.wait = timeout + time.Duration(rand.Int63n(int64(timeout)))
}

func (r *Raft) stepDown(term int) {
	if term > r.term {
		r.term = term
		r.votedFor = ""
	}
	r.role = follower
	r.leader = ""
	r.reset()
}

func (r *Raft) majority() int {
	return (len(r.peers)+1)/2 + 1
}

func (r *Raft) last() (int, int) {
	i := len(r.log) - 1
	return i, r.log[i].Term
}

func (r *Raft) campaign() {
	r.mu.Lock()
	r.term++
	r.role = candidate
	r.votedFor = r.id
	r.leader = ""
	r.reset()

	term := r.term
	idx, lastTerm := r.last()
	vote := Vote{Term: term, Candidate: r.id, LastIdx: idx, LastTerm: lastTerm}
	r.mu.Unlock()

	votes := make(chan bool, len(r.peers))
	for _, p := range r.peers {
		go func(p Peer) {
			reply, err := p.RequestVote(vote)
			r.mu.Lock()
			if err == nil && reply.Term > r.term {
				r.stepDown(reply.Term)
			}
			r.mu.Unlock()
			votes <- err == nil && reply.OK
		}(p)
	}

	won := 1
	deadline := time.After(timeout)
count:
	for range r.peers {
		select {
		case ok := <-votes:
			if ok {
				won++
			}
		case <-deadline:
			break count
		}
		if won >= r.majority() {
			break
		}
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.role != candidate || r.term != term || won < r.majority() {
		return
	}
	r.role = leader
	r.leader = r.id
	r.acked = time.Now()
	for id := range r.peers {
		r.next[id] = len(r.log)
		r.match[id] = 0
	}
	r.log = append(r.log, Entry{Term: r.term})
}

func (r *Raft) replicate() {
	r.mu.Lock()
	term := r.term
	r.sent = time.Now()
	r.mu.Unlock()

	acks := make(chan bool, len(r.peers))
	for id := range r.peers {
		go func(id string) { acks <- r.send(id, term) }(id)
	}

	alive := 1
	deadline := time.After(heartbeat)
count:
	for range r.peers {
		select {
		case ok := <-acks:
			if ok {
				alive++
			}
		case <-deadline:
			break count
		}
		if alive >= r.majority() {
			break
		}
	}

	r.mu.Lock()
	if r.role == leader && r.term == term && alive >= r.majority() {
		r.acked = time.Now()
		r.advance()
	}
	r.mu.Unlock()
}

func (r *Raft) send(id string, term int) bool {
	r.mu.Lock()
	if r.role != leader || r.term != term {
		r.mu.Unlock()
		return false
	}
	next := min(max(1, r.next[id]), len(r.log))
	msg := Append{
		Term:     term,
		Leader:   r.id,
		PrevIdx:  next - 1,
		PrevTerm: r.log[next-1].Term,
		Entries:  append([]Entry(nil), r.log[next:]...),
		Commit:   r.commit,
	}
	r.mu.Unlock()

	reply, err := r.peers[id].AppendEntries(msg)
	if err != nil {
		return false
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if reply.Term > r.term {
		r.stepDown(reply.Term)
		return false
	}
	if r.role != leader || r.term != term {
		return false
	}

	if reply.OK {
		r.match[id] = msg.PrevIdx + len(msg.Entries)
		r.next[id] = r.match[id] + 1
	} else {
		r.next[id] = max(1, reply.Hint)
	}
	return true
}

func (r *Raft) advance() {
	for n := len(r.log) - 1; n > r.commit && r.log[n].Term == r.term; n-- {
		count := 1
		for id := range r.peers {
			if r.match[id] >= n {
				count++
			}
		}
		if count >= r.majority() {
			r.commit = n
			return
		}
	}
}

func (r *Raft) flush() {
	r.mu.Lock()
	from := r.applied + 1
	ready := append([]Entry(nil), r.log[from:r.commit+1]...)
	r.applied = r.commit
	r.mu.Unlock()

	for i, e := range ready {
		if e.Cmd != nil {
			r.apply(from+i, e.Term, e.Cmd)
		}
	}
}

func (r *Raft) OnRequestVote(v Vote) Reply {
	r.mu.Lock()
	defer r.mu.Unlock()

	if v.Term < r.term {
		return Reply{Term: r.term}
	}
	if v.Term > r.term {
		r.stepDown(v.Term)
	}

	idx, term := r.last()
	behind := v.LastTerm < term || (v.LastTerm == term && v.LastIdx < idx)
	if behind || (r.votedFor != "" && r.votedFor != v.Candidate) {
		return Reply{Term: r.term}
	}

	r.votedFor = v.Candidate
	r.reset()
	return Reply{Term: r.term, OK: true}
}

func (r *Raft) OnAppendEntries(a Append) Reply {
	r.mu.Lock()
	defer r.mu.Unlock()

	if a.Term < r.term {
		return Reply{Term: r.term}
	}
	r.stepDown(a.Term)
	r.leader = a.Leader

	if a.PrevIdx >= len(r.log) {
		return Reply{Term: r.term, Hint: len(r.log)}
	}
	if bad := r.log[a.PrevIdx].Term; bad != a.PrevTerm {
		hint := a.PrevIdx
		for hint > 1 && r.log[hint-1].Term == bad {
			hint--
		}
		return Reply{Term: r.term, Hint: hint}
	}

	for i, e := range a.Entries {
		at := a.PrevIdx + 1 + i
		if at < len(r.log) && r.log[at].Term == e.Term {
			continue
		}
		r.log = append(r.log[:at], a.Entries[i:]...)
		break
	}

	if c := min(a.Commit, a.PrevIdx+len(a.Entries)); c > r.commit {
		r.commit = c
	}
	return Reply{Term: r.term, OK: true}
}
