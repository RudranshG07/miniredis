package main

import (
	"bufio"
	"bytes"
	"io"
	"os"
	"sync"

	"miniredis/resp"
)

type aof struct {
	mu    sync.Mutex
	f     *os.File
	fsync bool
}

func openAOF(path string, fsync bool) (*aof, [][]string, error) {
	data, err := os.ReadFile(path)
	if err != nil && !os.IsNotExist(err) {
		return nil, nil, err
	}

	src := bytes.NewReader(data)
	r := bufio.NewReader(src)
	var cmds [][]string
	good := 0
	for {
		args, err := resp.ReadCommand(r)
		if err != nil {
			break
		}
		cmds = append(cmds, args)
		good = len(data) - src.Len() - r.Buffered()
	}

	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, nil, err
	}
	if good < len(data) {
		if err := f.Truncate(int64(good)); err != nil {
			f.Close()
			return nil, nil, err
		}
	}
	return &aof{f: f, fsync: fsync}, cmds, nil
}

func (a *aof) write(args []string) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if _, err := io.WriteString(a.f, resp.Array(args)); err != nil {
		return err
	}
	if a.fsync {
		return a.f.Sync()
	}
	return nil
}
