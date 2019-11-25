package epoch

import (
	"time"
	"unsafe"
)

type GuardsInspector interface {
	Begin()
	Inspect(payload interface{}, active bool)
	End()
}

type NoOpInspector struct{}

func (i NoOpInspector) Begin()                                   {}
func (i NoOpInspector) Inspect(payload interface{}, active bool) {}
func (i NoOpInspector) End()                                     {}

type Guard struct {
	localEpoch atomicEpoch
	mgr        *ResourceManager
	deletions  []deletion
	payload    interface{}

	next unsafe.Pointer
}

func (h *Guard) Delete(resources []Resource) {
	globalEpoch := h.mgr.currentEpoch.load()
	h.deletions = append(h.deletions, deletion{
		epoch:     globalEpoch,
		resources: resources,
	})
}

func (h *Guard) Done() {
	h.localEpoch.store(h.localEpoch.load().deactivate())
}

type Resource interface {
	Delete() error
}

type ResourceManager struct {
	currentEpoch atomicEpoch

	// TODO: cache line size for non x86
	cachePad  [64]byte
	guards    guardList
	inspector GuardsInspector
}

func NewResourceManager(inspector GuardsInspector) *ResourceManager {
	rm := &ResourceManager{
		currentEpoch: atomicEpoch{epoch: 1 << 1},
		inspector:    inspector,
	}
	go rm.collectLoop()
	return rm
}

func (rm *ResourceManager) AcquireWithPayload(payload interface{}) *Guard {
	g := &Guard{
		mgr:     rm,
		payload: payload,
	}
	rm.guards.add(g)
	g.localEpoch.store(rm.currentEpoch.load().activate())
	return g
}

func (rm *ResourceManager) Acquire() *Guard {
	return rm.AcquireWithPayload(nil)
}

func (rm *ResourceManager) collectLoop() {
	ticker := time.NewTicker(100 * time.Millisecond)
	for range ticker.C {
		rm.collect()
	}
}

func (rm *ResourceManager) collect() {
	it := rm.guards.newIterator()
	canAdvance := true
	globalEpoch := rm.currentEpoch.load()
	remain := 0

	rm.inspector.Begin()
	for it.rewind(); it.valid(); it.next() {
		remain++
		guard := it.guard()
		localEpoch := guard.localEpoch.load()
		if localEpoch == 0 {
			// ignore newly added guard
			continue
		}

		isActive := localEpoch.isActive()
		rm.inspector.Inspect(guard.payload, isActive)

		if isActive {
			canAdvance = canAdvance && localEpoch.sub(globalEpoch) == 0
			continue
		}

		ds := guard.deletions[:0]
		for _, d := range guard.deletions {
			if globalEpoch.sub(d.epoch) < 2 {
				ds = append(ds, d)
				continue
			}
			for _, r := range d.resources {
				r.Delete()
			}
			d.resources = nil
		}
		guard.deletions = ds
		if len(guard.deletions) == 0 {
			it.delete()
			remain--
		}
	}
	rm.inspector.End()

	if canAdvance && remain != 0 {
		rm.currentEpoch.store(globalEpoch.successor())
	}
}

type deletion struct {
	epoch     epoch
	resources []Resource
}
