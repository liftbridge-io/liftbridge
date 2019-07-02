// Copyright 2017 Apcera Inc. All rights reserved.

package main

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/boltdb/bolt"
	"github.com/hashicorp/raft"
)

const keysBucket = "keys"

type operation struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// FSM represents the state machine logic exposed to the raft module.
type FSM struct {
	path string
	db   *bolt.DB
}

func NewFSM(path string) (*FSM, error) {
	fsm := &FSM{path: path}
	err := fsm.init()
	return fsm, err
}

func (f *FSM) init() error {
	db, err := bolt.Open(f.path, 0600, nil)
	if err != nil {
		return err
	}
	f.db = db
	return db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(keysBucket))
		return err
	})
}

func (f *FSM) Get(key []byte) ([]byte, error) {
	var val []byte
	err := f.db.View(func(tx *bolt.Tx) error {
		val = tx.Bucket([]byte(keysBucket)).Get(key)
		return nil
	})
	return val, err
}

func (f *FSM) Apply(l *raft.Log) interface{} {
	var op operation
	if err := json.Unmarshal(l.Data, &op); err != nil {
		panic(err)
	}

	if err := f.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte(keysBucket)).Put([]byte(op.Key), []byte(op.Value))
	}); err != nil {
		panic(err)
	}

	return nil
}

func (f *FSM) Restore(old io.ReadCloser) error {
	decoder := json.NewDecoder(old)
	for {
		op := &operation{}
		err := decoder.Decode(op)
		if err == io.EOF {
			fmt.Println("Done restoring from snapshot")
			break
		} else if err != nil {
			fmt.Printf("Error decoding stream during Restore: %s\n", err)
			return err
		}

		if err := f.db.Update(func(tx *bolt.Tx) error {
			return tx.Bucket([]byte(keysBucket)).Put([]byte(op.Key), []byte(op.Value))
		}); err != nil {
			fmt.Printf("Error during Restore: %s\n", err)
			return err
		}
	}

	return nil
}

func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	return &snapshot{db: f.db}, nil
}

type snapshot struct {
	db *bolt.DB
}

func (s *snapshot) Persist(sink raft.SnapshotSink) error {
	streamCh := make(chan []byte, 256)
	errorCh := make(chan error)

	go func() {
		s.db.View(func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte(keysBucket))
			return b.ForEach(func(k, v []byte) error {
				op := &operation{Key: string(k), Value: string(v)}
				data, err := json.Marshal(op)
				if err != nil {
					errorCh <- err
					return err
				}
				streamCh <- data
				return nil
			})
		})
		close(streamCh)
	}()

	for {
		select {
		case raw, ok := <-streamCh:
			if !ok {
				return sink.Close()
			}
			sink.Write(raw)
		case err := <-errorCh:
			sink.Cancel()
			return err
		}
	}
}

func (s *snapshot) Release() {
}
