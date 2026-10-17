package main

import (
	"sync"
	"time"
)

type entry struct {
	value   string
	expires time.Time
}

func (e entry) expired() bool {
	return !e.expires.IsZero() && time.Now().After(e.expires)
}

type store struct {
	mu   sync.RWMutex
	data map[string]entry
}

func newStore() *store {
	return &store{data: map[string]entry{}}
}

func (s *store) lookup(key string) (entry, bool) {
	s.mu.RLock()
	e, ok := s.data[key]
	s.mu.RUnlock()

	if ok && e.expired() {
		s.mu.Lock()
		if cur, ok := s.data[key]; ok && cur.expired() {
			delete(s.data, key)
		}
		s.mu.Unlock()
		return entry{}, false
	}
	return e, ok
}

func (s *store) get(key string) (string, bool) {
	e, ok := s.lookup(key)
	return e.value, ok
}

func (s *store) expiry(key string) (time.Time, bool) {
	e, ok := s.lookup(key)
	return e.expires, ok
}

func (s *store) set(key, value string, ttl time.Duration) {
	e := entry{value: value}
	if ttl > 0 {
		e.expires = time.Now().Add(ttl)
	}

	s.mu.Lock()
	s.data[key] = e
	s.mu.Unlock()
}

func (s *store) del(keys ...string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	n := 0
	for _, key := range keys {
		if _, ok := s.data[key]; ok {
			delete(s.data, key)
			n++
		}
	}
	return n
}

func (s *store) keys() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make([]string, 0, len(s.data))
	for key, e := range s.data {
		if !e.expired() {
			out = append(out, key)
		}
	}
	return out
}

func (s *store) flush() {
	s.mu.Lock()
	s.data = map[string]entry{}
	s.mu.Unlock()
}
