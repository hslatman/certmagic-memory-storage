package storage

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/caddyserver/certmagic"
)

func TestStorage(t *testing.T) {
	// set up your storage
	memory := New()
	// create a new test suite runner with rng initialized to 0
	fixedSuite := newTestSuite(memory)
	// then run the tests on it
	fixedSuite.run(t)

	// create a new test suite runner with rng initialized unix timestamp
	randomSuite := newTestSuite(memory)
	randomSuite.rng = rand.New(rand.NewSource(time.Now().UTC().UnixNano()))
	// run the random suite
	randomSuite.run(t)
}

// The code below was copied from https://github.com/oyato/certmagic-storage-tests
// and adapted to the new certmagic.Storage interface, taking a context.Context as
// first input to the methods.
//
// Package tests implements a suite of tests for certmagic.Storage
//
//
// Example Usage:
//
// package storage
//
// import (
//     tests "github.com/oyato/certmagic-storage-tests"
//     "testing"
// )
//
// func TestStorage(t *testing.T) {
//     // set up your storage
//     storage := NewInstanceOfYourStorage()
//     // then run the tests on it
//     tests.NewTestSuite(storage).Run(t)
// }
//

var (
	// keyPrefix is prepended to all tested keys.
	// If changed, it must not contain a forward slash (/)
	keyPrefix = "__test__key__"
)

// suite implements tests for certmagic.Storage.
//
// Users should call suite.Run() in their storage_test.go file.
type suite struct {
	s   certmagic.Storage
	rng interface{ Int() int }

	mu       sync.Mutex
	randKeys []string
}

// Run tests the Storage
//
// NOTE: t.Helper() is not called.
//
//	Test failure line numbers will be reported on files inside this package.
func (ts *suite) run(t *testing.T) {
	t.Cleanup(func() {
		ts.mu.Lock()
		defer ts.mu.Unlock()
		ctx := context.Background()
		for _, k := range ts.randKeys {
			ts.s.Delete(ctx, k)
		}
	})
	ts.testLocker(t)
	ts.testStorageSingleKey(t)
	ts.testStorageDir(t)
}

func (ts *suite) testLocker(t *testing.T) {
	ctx := context.Background()
	key := strconv.Itoa(ts.rng.Int())
	if err := ts.s.Unlock(ctx, key); err == nil {
		t.Fatalf("Storage successfully unlocks unlocked key")
	}
	if err := ts.s.Lock(ctx, key); err != nil {
		t.Fatalf("Storage fails to lock key: %s", err)
	}
	if err := ts.s.Unlock(ctx, key); err != nil {
		t.Fatalf("Storage fails to unlock locked key: %s", err)
	}

	test := func(key string) {
		for i := 0; i < 5; i++ {
			if err := ts.s.Lock(ctx, key); err != nil {
				// certmagic lockers can timeout
				continue
			}
			runtime.Gosched()
			if err := ts.s.Unlock(ctx, key); err != nil {
				t.Fatalf("Storage.Unlock failed: %s", err)
			}
		}
	}
	wg := &sync.WaitGroup{}
	for i := 0; i < 5; i++ {
		key := ts.randKey()
		for j := 0; j < 2; j++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				test(key)
			}()
		}
	}
	wg.Wait()
}

func (ts *suite) testStorageSingleKey(t *testing.T) {
	ctx := context.Background()
	key := ts.randKey()
	val := []byte(key)
	sto := ts.s
	sto.Lock(ctx, key)
	defer sto.Unlock(ctx, key)

	if sto.Exists(ctx, key) {
		t.Fatalf("Un-stored key %s exists", key)
	}

	if _, err := sto.Load(ctx, key); err == nil {
		t.Fatalf("Load(%s) should fail: the key was not stored", key)
	}

	if _, err := sto.Stat(ctx, key); err == nil {
		t.Fatalf("Stat(%s) should fail: the key doesn't exist", key)
	}

	if err := sto.Store(ctx, "", []byte{}); err == nil {
		t.Fatalf("Store() with empty key should fail")
	}

	if err := sto.Store(ctx, key, nil); err != nil {
		t.Fatalf("Store(%s) with `nil` value failed: %s", key, err)
	}

	if err := sto.Store(ctx, key, []byte{}); err != nil {
		t.Fatalf("Store(%s) with empty value failed: %s", key, err)
	}

	if err := sto.Store(ctx, key, val); err != nil {
		t.Fatalf("Store(%s) failed: %s", key, err)
	}

	if !sto.Exists(ctx, key) {
		t.Fatalf("Stored key %s doesn't exists", key)
	}

	switch s, err := sto.Load(ctx, key); {
	case err != nil:
		t.Fatalf("Load(%s) failed: %s", key, err)
	case !bytes.Equal(val, s):
		t.Fatalf("Load(%s) failed: loaded %#v != stored %#v", key, s, val)
	}

	if err := sto.Delete(ctx, key); err != nil {
		t.Fatalf("Delete(%s) failed: %s", key, err)
	}

	if sto.Exists(ctx, key) {
		t.Fatalf("Deleted key still %s exists", key)
	}
}

func (ts *suite) testStorageDir(t *testing.T) {
	ctx := context.Background()
	sto := ts.s
	dir := ts.randKey()
	val := []byte(dir)
	k1 := dir + "/k1"
	k2 := dir + "/k/a/b"
	k3 := dir + "/k/c"
	ts.mu.Lock()
	ts.randKeys = append(ts.randKeys, k1, k2, k3)
	ts.mu.Unlock()

	if _, err := sto.List(ctx, k1, true); err == nil {
		t.Fatalf("List(%s, true) should fail: the key doesn't exist", k1)
	}

	if _, err := sto.List(ctx, k2, false); err == nil {
		t.Fatalf("List(%s, false) should fail: the key doesn't exist", k2)
	}

	if err := sto.Store(ctx, k1, val); err != nil {
		t.Fatalf("Store(%s) failed: %s", k1, err)
	}
	if err := sto.Store(ctx, k2, val); err != nil {
		t.Fatalf("Store(%s) failed: %s", k2, err)
	}
	if err := sto.Store(ctx, k3, val); err != nil {
		t.Fatalf("Store(%s) failed: %s", k3, err)
	}

	switch inf, err := sto.Stat(ctx, dir); {
	case err != nil:
		t.Fatalf("Stat(%s) failed: %s", dir, err)
	case inf.Key != dir:
		t.Fatalf("Stat(%s) failed: Key is set to %#v", dir, inf.Key)
	case inf.IsTerminal:
		t.Fatalf("Stat(%s) failed: IsTerminal should be false for directory keys", dir)
	}

	switch inf, err := sto.Stat(ctx, k2); {
	case err != nil:
		t.Fatalf("Stat(%s) failed: %s", k2, err)
	case inf.Key != k2:
		t.Fatalf("Stat(%s) failed: Key is set to %#v, but should be %#v", k2, inf.Key, k2)
	case !inf.IsTerminal:
		t.Fatalf("Stat(%s) failed: IsTerminal should be true for non-directory keys", k2)
	}

	if ls, err := sto.List(ctx, dir, false); err != nil {
		t.Fatalf("List(%s, false) failed: %s", dir, err)
	} else {
		sort.Strings(ls)
		got := fmt.Sprintf("%#v", ls)
		exp := fmt.Sprintf("%#v", []string{dir + "/k", k1})
		if got != exp {
			t.Fatalf("List(%s, false) failed: it should return %s, not %s", dir, exp, got)
		}
	}

	if ls, err := sto.List(ctx, dir, true); err != nil {
		t.Fatalf("List(%s, true) failed: %s", dir, err)
	} else {
		sort.Strings(ls)
		got := fmt.Sprintf("%#v", ls)
		exp := fmt.Sprintf("%#v", []string{
			dir + "/k",
			dir + "/k/a",
			dir + "/k/a/b",
			dir + "/k/c",
			k1,
		})
		if got != exp {
			t.Fatalf("List(%s, true) failed: it should return %s, not %s", dir, exp, got)
		}
	}
}

func (ts *suite) randKey() string {
	return keyPrefix + strconv.Itoa(ts.rng.Int())
}

// newTestSuite returns a new Suite initalised with storage s
// and a `rand.New(rand.NewSource(0))` random number generator
func newTestSuite(s certmagic.Storage) *suite {
	return &suite{
		s:   s,
		rng: rand.New(rand.NewSource(0)),
	}
}
