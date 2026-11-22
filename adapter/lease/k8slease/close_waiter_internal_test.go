package k8slease

import (
	"errors"
	"runtime"
	"sync"
	"testing"
	"time"
)

// TestCloseWaiterIsShared pins the invariant behind the lazy waiter: every
// Close call observes the same channel, including concurrent ones. Close can
// time out and be retried, so creating a waiter per call would accumulate
// goroutines blocked on the same WaitGroup.
// TestCloseWaiterIsShared 固定惰性等待者的不变量：每次 Close 都看到同一个 channel，
// 并发调用也不例外。Close 可能超时后重试，每次新建等待者会累积阻塞在同一个 WaitGroup 上的
// goroutine。
func TestCloseWaiterIsShared(t *testing.T) {
	l := &Locker{}
	first := l.closeWaiter()
	if second := l.closeWaiter(); first != second {
		t.Fatal("closeWaiter created a second channel")
	}

	const callers = 8
	channels := make([]<-chan struct{}, callers)
	var wg sync.WaitGroup
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			channels[i] = l.closeWaiter()
		}(i)
	}
	wg.Wait()
	for i, ch := range channels {
		if ch != first {
			t.Fatalf("caller %d got a different waiter", i)
		}
	}
}

// TestCloseTimeoutDoesNotAccumulateWaiters is the regression for the leak: an
// operation that outlives several Close calls used to leave one goroutine per
// call blocked on inflight.Wait. Twenty timeouts must not add twenty waiters.
// TestCloseTimeoutDoesNotAccumulateWaiters 是泄漏的回归点：一次比多次 Close 活得更久的
// 操作，过去会让每次调用留下一个阻塞在 inflight.Wait 上的 goroutine。二十次超时不能新增
// 二十个等待者。
func TestCloseTimeoutDoesNotAccumulateWaiters(t *testing.T) {
	l := &Locker{timeout: time.Millisecond}
	l.inflight.Add(1)

	base := runtime.NumGoroutine()
	for i := 0; i < 20; i++ {
		if err := l.Close(); !errors.Is(err, ErrCloseTimeout) {
			t.Fatalf("Close #%d = %v, want ErrCloseTimeout", i, err)
		}
	}
	added := runtime.NumGoroutine() - base
	if added > 3 {
		t.Fatalf("20 timeouts added %d goroutines, want at most 3", added)
	}

	l.inflight.Done()
	select {
	case <-l.closeWaiter():
	case <-time.After(time.Second):
		t.Fatal("waiter was not released after the in-flight operation finished")
	}
}
