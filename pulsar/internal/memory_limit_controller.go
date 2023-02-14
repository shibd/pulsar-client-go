package internal

import (
	"sync"
	"sync/atomic"
)

type MemoryLimitController interface {
	ReserveMemory(size uint64)
	TryReserveMemory(size uint64) bool
	ForceReserveMemory(size uint64)
	ReleaseMemory(size uint64)
	CurrentUsage() uint64
	CurrentUsagePercent() float64
	IsMemoryLimited() bool
}

type memoryLimitController struct {
	limit        uint64
	currentUsage uint64
	mutex        sync.Mutex
	cond         sync.Cond
}

func NewMemoryLimitController(limit uint64) MemoryLimitController {
	mlc := &memoryLimitController{
		limit: limit,
		mutex: sync.Mutex{},
	}
	mlc.cond = *sync.NewCond(&mlc.mutex)
	return mlc
}

func (m *memoryLimitController) ReserveMemory(size uint64) {
	if !m.TryReserveMemory(size) {
		m.cond.L.Lock()
		defer m.cond.L.Unlock()

		for !m.TryReserveMemory(size) {
			m.cond.Wait()
		}
	}
}

func (m *memoryLimitController) TryReserveMemory(size uint64) bool {
	for {
		current := atomic.LoadUint64(&m.currentUsage)
		newUsage := current + size

		// This condition means we allowed one request to go over the limit.
		if m.IsMemoryLimited() && current > m.limit {
			return false
		}

		if atomic.CompareAndSwapUint64(&m.currentUsage, current, newUsage) {
			return true
		}
	}
	return false
}

func (m *memoryLimitController) ForceReserveMemory(size uint64) {
	atomic.AddUint64(&m.currentUsage, size)
}

func (m *memoryLimitController) ReleaseMemory(size uint64) {
	newUsage := atomic.AddUint64(&m.currentUsage, -size)
	if newUsage+size > m.limit && newUsage <= m.limit {
		m.cond.Broadcast()
	}
}

func (m *memoryLimitController) CurrentUsage() uint64 {
	return atomic.LoadUint64(&m.currentUsage)
}

func (m *memoryLimitController) CurrentUsagePercent() float64 {
	return float64(atomic.LoadUint64(&m.currentUsage)) / float64(m.limit)
}

func (m *memoryLimitController) IsMemoryLimited() bool {
	return m.limit > 0
}
