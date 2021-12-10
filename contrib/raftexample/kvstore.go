// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"log"
	"strings"
	"sync"

	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/etcdserver/api/snap"
)

type Schema map[string]interface{}

// key: username
// value: password
type Users map[string]string

// key: username
// value: [followeename]
type Following map[string][]string

// key: author
// value: [text]
type Posts map[string][]string

// a key-value store backed by raft
type kvstore struct {
	proposeC    chan<- string // channel for proposing updates
	mu          sync.RWMutex
	kvStore     Schema // current committed key-value pairs
	snapshotter *snap.Snapshotter
}

type kv struct {
	Key string
	Val string
}

func newKVStore(snapshotter *snap.Snapshotter, proposeC chan<- string, commitC <-chan *commit, errorC <-chan error) *kvstore {
	dbstore := map[string]interface{}{
		"users": Users{
			"test":     "$2a$16$FQ1.75Cr3XASYvg0Hz8dCOHzoVS88QyvlR6I.S0KESduNoyYtua7C", // test123
			"user":     "$2a$16$x/gu0Dv14nsX8jvz//TJ3OcmPr0F.Yy7Qt67y1Tqz7ngxblnVuJy6", // passwd
			"follower": "$2a$16$ldQoU6bE2XZFAodgRHLnHutTH3dit5CWWsa8yFKUNHEHtmNa8yMT6", // follower
		},
		"following": Following{
			"follower": []string{"test", "user"},
		},
		"posts": Posts{
			"test": []string{"post created by the user \"test\""},
			"user": []string{"post created by the user \"user\""},
		},
	}
	s := &kvstore{proposeC: proposeC, kvStore: dbstore, snapshotter: snapshotter}
	snapshot, err := s.loadSnapshot()
	if err != nil {
		log.Panic(err)
	}
	if snapshot != nil {
		log.Printf("loading snapshot at term %d and index %d", snapshot.Metadata.Term, snapshot.Metadata.Index)
		if err := s.recoverFromSnapshot(snapshot.Data); err != nil {
			log.Panic(err)
		}
	}
	// read commits from raft into kvStore map until error
	go s.readCommits(commitC, errorC)
	return s
}

func (s *kvstore) Lookup(key string) (interface{}, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// login 	 	 -> key: /users/<username>
	// get_users 	 -> key: /users
	// get_following -> key: /following/<username>
	// get_posts 	 -> key: /posts/<author>
	if strings.HasPrefix(key, "/users") {
		usersStore := s.kvStore["users"].(Users)
		if len(key) == len("/users") {
			// get_users
			log.Printf("getting users")
			users := make([]string, 0, len(usersStore))
			for u := range usersStore {
				users = append(users, u)
			}
			return users, true
		} else {
			// login
			username := key[len("/users")+1:]
			log.Printf("%s loggin in", username)
			passwd, ok := usersStore[username]
			return passwd, ok
		}
	} else if strings.HasPrefix(key, "/following") {
		// get_following
		followingStore := s.kvStore["following"].(Following)
		username := key[len("/following")+1:]
		return followingStore[username], true
	} else if strings.HasPrefix(key, "/posts") {
		// get_posts
		postsStore := s.kvStore["posts"].(Posts)
		username := key[len("/posts")+1:]
		return postsStore[username], true
	}
	return nil, true
}

func (s *kvstore) Propose(k string, v string) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(kv{k, v}); err != nil {
		log.Fatal(err)
	}
	s.proposeC <- buf.String()
}

func (s *kvstore) readCommits(commitC <-chan *commit, errorC <-chan error) {
	for commit := range commitC {
		if commit == nil {
			// signaled to load snapshot
			snapshot, err := s.loadSnapshot()
			if err != nil {
				log.Panic(err)
			}
			if snapshot != nil {
				log.Printf("loading snapshot at term %d and index %d", snapshot.Metadata.Term, snapshot.Metadata.Index)
				if err := s.recoverFromSnapshot(snapshot.Data); err != nil {
					log.Panic(err)
				}
			}
			continue
		}

		for _, data := range commit.data {
			var dataKv kv
			dec := gob.NewDecoder(bytes.NewBufferString(data))
			if err := dec.Decode(&dataKv); err != nil {
				log.Fatalf("raftexample: could not decode message (%v)", err)
			}
			log.Printf("dataKv: %v", dataKv)

			s.mu.Lock()
			// signup	  -> key: /users/<username>, value: <password>
			// follow	  -> key: /following/<username>, value: <username>
			// unfollow	  -> key: /following/un/<username>, value: <username>
			// createpost -> key: /posts/<username>, value: <text>
			s.parseValue(dataKv.Key, dataKv.Val)
			s.mu.Unlock()
		}
		close(commit.applyDoneC)
	}
	if err, ok := <-errorC; ok {
		log.Fatal(err)
	}
}

func (s *kvstore) parseValue(key, value string) {
	// remove extra double quote
	if value[0] == '"' && value[len(value)-1] == '"' {
		value = value[1 : len(value)-1]
	}

	if strings.HasPrefix(key, "/users") {
		// signup
		userStore := s.kvStore["users"].(Users)
		username := key[len("/users")+1:]
		userStore[username] = value
	} else if strings.HasPrefix(key, "/following") {
		followingStore := s.kvStore["following"].(Following)
		key = key[len("/following"):]
		if strings.HasPrefix(key, "/un") {
			// unfollow (search & remove)
			username := key[len("/un")+1:]
			log.Printf("%s unfollowed %s", username, value)
			log.Printf("%s's following %v", username, followingStore[username])
			for i, user := range followingStore[username] {
				if user == value {
					followingStore[username] = append(followingStore[username][:i], followingStore[username][i+1:]...)
					break
				}
			}
			log.Printf("%s's following %v", username, followingStore[username])
		} else {
			// follow
			username := key[1:]
			followingStore[username] = append(followingStore[username], value)
			log.Printf("%s followed %s", username, value)
			log.Printf("%s's following %v", username, followingStore[username])
		}
	} else if strings.HasPrefix(key, "/posts") {
		postsStore := s.kvStore["posts"].(Posts)
		username := key[len("/posts")+1:]
		postsStore[username] = append(postsStore[username], value)
	}
}

func (s *kvstore) getSnapshot() ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return json.Marshal(s.kvStore)
}

func (s *kvstore) loadSnapshot() (*raftpb.Snapshot, error) {
	snapshot, err := s.snapshotter.Load()
	if err == snap.ErrNoSnapshot {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return snapshot, nil
}

func (s *kvstore) recoverFromSnapshot(snapshot []byte) error {
	var store Schema
	if err := json.Unmarshal(snapshot, &store); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.kvStore = store
	return nil
}
