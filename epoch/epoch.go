package epoch

import "sync/atomic"

type epoch uint64

func (e epoch) isActive() bool {
	return uint64(e)&1 == 1
}

func (e epoch) activate() epoch {
	return epoch(uint64(e) | 1)
}

func (e epoch) deactivate() epoch {
	return epoch(uint64(e) & ^uint64(1))
}

func (e epoch) sub(a epoch) int {
	return int(uint64(e)-(uint64(a) & ^uint64(1))) >> 1
}

func (e epoch) successor() epoch {
	return epoch(uint64(e) + 2)
}

type atomicEpoch struct {
	epoch uint64
}

func (e *atomicEpoch) load() epoch {
	return (epoch)(atomic.LoadUint64(&e.epoch))
}

func (e *atomicEpoch) store(new epoch) {
	atomic.StoreUint64(&e.epoch, uint64(new))
}
