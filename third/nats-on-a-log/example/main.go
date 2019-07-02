// Copyright 2017 Apcera Inc. All rights reserved.

package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb"
	"github.com/nats-io/nats.go"
)

type joinRequest struct {
	ID string `json:"id"`
}

type joinResponse struct {
	OK    bool   `json:"ok"`
	Error string `json:"error"`
}

func main() {
	var (
		port         = flag.String("port", "8080", "http port")
		id           = flag.String("id", "", "node id")
		kvPath       = flag.String("kv-path", "", "KV store path")
		logPath      = flag.String("log-path", "", "Raft log path")
		snapshotPath = flag.String("sn-path", "", "Log snapshot path")
		join         = flag.String("join", "", "Join address (leave empty to seed")
	)
	flag.Parse()

	if *kvPath == "" || *logPath == "" || *snapshotPath == "" {
		panic("path not provided")
	}

	var (
		config   = raft.DefaultConfig()
		fsm, err = NewFSM(*kvPath)
		natsOpts = nats.DefaultOptions
	)
	if err != nil {
		panic(err)
	}
	config.LocalID = raft.ServerID(*id)

	store, err := raftboltdb.NewBoltStore(*logPath)
	if err != nil {
		panic(err)
	}

	cacheStore, err := raft.NewLogCache(512, store)
	if err != nil {
		panic(err)
	}

	snapshots, err := raft.NewFileSnapshotStore(*snapshotPath, 2, os.Stdout)
	if err != nil {
		panic(err)
	}

	conn, err := natsOpts.Connect()
	if err != nil {
		panic(err)
	}

	trans, err := natslog.NewNATSTransport(*id, "", conn, 2*time.Second, os.Stdout)
	if err != nil {
		panic(err)
	}

	node, err := raft.NewRaft(config, fsm, cacheStore, store, snapshots, trans)
	if err != nil {
		panic(err)
	}

	// Bootstrap if no node to join is provided and there is no existing state.
	existingState, err := raft.HasExistingState(cacheStore, store, snapshots)
	if err != nil {
		panic(err)
	}
	if *join == "" && !existingState {
		config := raft.Configuration{
			Servers: []raft.Server{
				raft.Server{
					ID:      raft.ServerID(*id),
					Address: raft.ServerAddress(*id),
				},
			},
		}
		if err := node.BootstrapCluster(config).Error(); err != nil {
			panic(err)
		}
	}

	// Handle join requests.
	if _, err := conn.Subscribe(fmt.Sprintf("%s.join", *id), func(msg *nats.Msg) {
		var req joinRequest
		if err := json.Unmarshal(msg.Data, &req); err != nil {
			resp, _ := json.Marshal(&joinResponse{OK: false, Error: err.Error()})
			conn.Publish(msg.Reply, resp)
			return
		}
		future := node.AddVoter(raft.ServerID(req.ID), raft.ServerAddress(req.ID), 0, 0)
		resp := &joinResponse{OK: true}
		if err := future.Error(); err != nil {
			resp.OK = false
			resp.Error = err.Error()
		}
		r, _ := json.Marshal(resp)
		conn.Publish(msg.Reply, r)
	}); err != nil {
		panic(err)
	}

	go func() {
		for isLeader := range node.LeaderCh() {
			if isLeader {
				fmt.Println("*** LEADERSHIP ACQUIRED ***")
				bar := node.Barrier(0)
				if err := bar.Error(); err != nil {
					fmt.Printf("Failed applying barrier when becoming leader: %s\n", err)
				}
			} else {
				fmt.Println("*** LEADERSHIP LOST ***")
			}
		}
	}()

	// If node to join was provided, add it to the cluster.
	if *join != "" {
		req, _ := json.Marshal(&joinRequest{ID: *id})
		resp, err := conn.Request(fmt.Sprintf("%s.join", *join), req, 5*time.Second)
		if err != nil {
			panic(err)
		}
		var r joinResponse
		if err := json.Unmarshal(resp.Data, &r); err != nil {
			panic(err)
		}
		if !r.OK {
			panic(r.Error)
		}
	}

	http.HandleFunc("/set", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			http.Error(w, "Must use POST request", http.StatusBadRequest)
			return
		}
		defer r.Body.Close()
		var buf bytes.Buffer
		_, err := buf.ReadFrom(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		var op operation
		if err := json.Unmarshal(buf.Bytes(), &op); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if string(op.Key) == "" {
			http.Error(w, "Key not set", http.StatusBadRequest)
			return
		}
		future := node.Apply(buf.Bytes(), 5*time.Second)
		if err := future.Error(); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Write([]byte(strconv.FormatInt(int64(future.Index()), 10)))
	})

	http.HandleFunc("/get/", func(w http.ResponseWriter, r *http.Request) {
		key := strings.TrimPrefix(r.URL.Path, "/get/")
		val, err := fsm.Get([]byte(key))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if val == nil {
			http.Error(w, "", http.StatusNotFound)
			return
		}
		w.Write(val)
	})

	if err := http.ListenAndServe(":"+*port, nil); err != nil {
		panic(err)
	}
}
