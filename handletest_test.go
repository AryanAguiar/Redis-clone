package main

import (
	"strconv"
	"sync"
	"testing"
)

func TestConcurrentIncr(t *testing.T) {
	SETs = map[string]string{}

	var wg sync.WaitGroup
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			incr([]Value{{bulk: "counter"}})
		}()
	}
	wg.Wait()

	result := get([]Value{{bulk: "counter"}})
	if result.bulk != "1000" {
		t.Errorf("expected 1000, got %s", result.bulk)
	}
}

func TestConcurrentSet(t *testing.T) {
	SETs = map[string]string{}

	var wg sync.WaitGroup
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			set([]Value{{bulk: "counter"}, {bulk: "1000"}})
		}()
	}
	wg.Wait()

	result := get([]Value{{bulk: "counter"}})
	if result.bulk != "1000" {
		t.Errorf("expected 1000, got %s", result.bulk)
	}
}

func TestConcurrentSadd(t *testing.T) {
	Sets = map[string]map[string]struct{}{}

	var wg sync.WaitGroup
	for i := 0; i < 1000; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			sadd([]Value{{bulk: "myset"}, {bulk: strconv.Itoa(i)}})
		}()
	}
	wg.Wait()

	result := scard([]Value{{bulk: "myset"}})
	if result.num != 1000 {
		t.Errorf("expected 1000, got %d", result.num)
	}
}
