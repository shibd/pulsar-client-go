package internal

import (
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
	"time"
)

func TestLimit(t *testing.T) {

	mlc := NewMemoryLimitController(100)

	for i := 0; i < 101; i++ {
		assert.True(t, mlc.TryReserveMemory(1))
	}

	assert.False(t, mlc.TryReserveMemory(1))
	assert.Equal(t, uint64(101), mlc.CurrentUsage())
	assert.Equal(t, 1.01, mlc.CurrentUsagePercent())

	mlc.ReleaseMemory(1)
	assert.Equal(t, uint64(100), mlc.CurrentUsage())
	assert.Equal(t, 1.0, mlc.CurrentUsagePercent())

	assert.True(t, mlc.TryReserveMemory(1))
	assert.Equal(t, uint64(101), mlc.CurrentUsage())

	mlc.ForceReserveMemory(99)
	assert.False(t, mlc.TryReserveMemory(1))
	assert.Equal(t, uint64(200), mlc.CurrentUsage())
	assert.Equal(t, 2.0, mlc.CurrentUsagePercent())

	mlc.ReleaseMemory(50)
	assert.False(t, mlc.TryReserveMemory(1))
	assert.Equal(t, uint64(150), mlc.CurrentUsage())
	assert.Equal(t, 1.5, mlc.CurrentUsagePercent())
}

func TestMultiGoroutineTryReserveMem(t *testing.T) {
	mlc := NewMemoryLimitController(10000)

	// Multi goroutine try reserve memory.
	wg := sync.WaitGroup{}
	task := func() {
		for i := 0; i < 1000; i++ {
			assert.True(t, mlc.TryReserveMemory(1))
		}
		wg.Done()
	}
	for i := 0; i < 10; i++ {
		go task()
		wg.Add(1)
	}
	assert.True(t, mlc.TryReserveMemory(1))
	wg.Wait()
	assert.False(t, mlc.TryReserveMemory(1))
	assert.Equal(t, uint64(10001), mlc.CurrentUsage())
	assert.Equal(t, 1.0001, mlc.CurrentUsagePercent())
}

func TestBlocking(t *testing.T) {
	mlc := NewMemoryLimitController(100)
	assert.True(t, mlc.TryReserveMemory(101))
	assert.Equal(t, uint64(101), mlc.CurrentUsage())
	assert.Equal(t, 1.01, mlc.CurrentUsagePercent())

	reserveMemory := func(ch chan int) {
		mlc.ReserveMemory(1)
		ch <- 1
	}

	await := func(ch chan int, timeOut time.Duration) bool {
		select {
		case <-ch:
			return true
		case <-time.After(timeOut):
			return false
		}
	}

	gorNum := 10
	chs := make([]chan int, gorNum)
	for i := 0; i < gorNum; i++ {
		chs[i] = make(chan int, 1)
		go reserveMemory(chs[i])
	}

	// The threads are blocked since the quota is full
	for i := 0; i < gorNum; i++ {
		assert.False(t, await(chs[i], 100*time.Millisecond))
	}
	assert.Equal(t, uint64(101), mlc.CurrentUsage())

	mlc.ReleaseMemory(uint64(gorNum))
	for i := 0; i < gorNum; i++ {
		assert.True(t, await(chs[i], 100*time.Millisecond))
	}
	assert.Equal(t, uint64(101), mlc.CurrentUsage())
}

func TestStepRelease(t *testing.T) {
	mlc := NewMemoryLimitController(100)
	assert.True(t, mlc.TryReserveMemory(101))
	assert.Equal(t, uint64(101), mlc.CurrentUsage())
	assert.Equal(t, 1.01, mlc.CurrentUsagePercent())

	reserveMemory := func(ch chan int) {
		mlc.ReserveMemory(1)
		ch <- 1
	}

	await := func(ch chan int, timeOut time.Duration) bool {
		select {
		case <-ch:
			return true
		case <-time.After(timeOut):
			return false
		}
	}

	gorNum := 10
	ch := make(chan int, 1)
	for i := 0; i < gorNum; i++ {
		go reserveMemory(ch)
	}

	// The threads are blocked since the quota is full
	assert.False(t, await(ch, 100*time.Millisecond))
	assert.Equal(t, uint64(101), mlc.CurrentUsage())

	for i := 0; i < gorNum; i++ {
		mlc.ReleaseMemory(1)
		assert.True(t, await(ch, 100*time.Millisecond))
		assert.False(t, await(ch, 100*time.Millisecond))
	}
	assert.Equal(t, uint64(101), mlc.CurrentUsage())
}
