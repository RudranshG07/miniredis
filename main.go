package main

import (
	"flag"
	"log"
	"net"
)

func main() {
	addr := flag.String("addr", ":6380", "listen address")
	path := flag.String("aof", "appendonly.aof", "append only file")
	fsync := flag.Bool("fsync", true, "fsync every write")
	nodes := flag.String("nodes", "", "run as a cluster, redis=raft address pairs, comma separated")
	flag.Parse()

	s := &server{db: newStore()}

	if *nodes != "" {
		n, err := join(s, *addr, *nodes)
		if err != nil {
			log.Fatal(err)
		}
		s.node = n
		log.Printf("cluster node, raft on %s", n.ln.Addr())
	} else {
		a, cmds, err := openAOF(*path, *fsync)
		if err != nil {
			log.Fatal(err)
		}
		defer a.f.Close()

		for _, args := range cmds {
			s.exec(args)
		}
		s.aof = a
		if len(cmds) > 0 {
			log.Printf("replayed %d commands", len(cmds))
		}
	}

	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("listening on %s", *addr)
	s.serve(ln)
}
