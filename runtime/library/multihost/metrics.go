package multihost

import "sync/atomic"

// Counters is the multi-host instrument: backend existence-query
// counts (0045 amplification), served read verbs, reconcile actions,
// the negative cache, and the 0049 locked-write transaction profile.
// Everything is a plain atomic counter; this is a lab/observability
// instrument, not a Prometheus integration.
type Counters struct {
	// Backend existence-query calls (0045).
	BackendGet  atomic.Uint64
	BackendList atomic.Uint64

	// Served read verbs (for amplification ratios).
	ServedGet  atomic.Uint64
	ServedList atomic.Uint64

	// Reconcile actions, split by where they fired (0045).
	AdoptInline   atomic.Uint64
	AdoptSweep    atomic.Uint64
	CollectInline atomic.Uint64
	CollectSweep  atomic.Uint64

	// CollectSkippedAge counts orphan records left in place because
	// they are younger than the minAge grace window.
	CollectSkippedAge atomic.Uint64

	// Negative cache (0045).
	NegCacheHit  atomic.Uint64
	NegCacheMiss atomic.Uint64

	// 0049 locked-write transaction instrumentation. WriteRetry counts
	// every transaction retry attempt (a write that hit a CAS conflict
	// and re-read). WriteOK / WriteConflict count served writes by
	// terminal outcome. MaxWriteDepth is the high-water retry depth.
	WriteRetry    atomic.Uint64
	WriteOK       atomic.Uint64
	WriteConflict atomic.Uint64
	MaxWriteDepth atomic.Uint64
}

// Snapshot is a point-in-time, JSON-renderable view of the counters.
type Snapshot struct {
	BackendGet        uint64 `json:"backendGet"`
	BackendList       uint64 `json:"backendList"`
	ServedGet         uint64 `json:"servedGet"`
	ServedList        uint64 `json:"servedList"`
	AdoptInline       uint64 `json:"adoptInline"`
	AdoptSweep        uint64 `json:"adoptSweep"`
	CollectInline     uint64 `json:"collectInline"`
	CollectSweep      uint64 `json:"collectSweep"`
	CollectSkippedAge uint64 `json:"collectSkippedAge"`
	NegCacheHit       uint64 `json:"negCacheHit"`
	NegCacheMiss      uint64 `json:"negCacheMiss"`

	WriteRetry    uint64 `json:"writeRetry"`
	WriteOK       uint64 `json:"writeOk"`
	WriteConflict uint64 `json:"writeConflict"`
	MaxWriteDepth uint64 `json:"maxWriteDepth"`

	// GetAmplification is BackendGet/ServedGet. 1.0 means no caching
	// (the read-path-reconcile baseline); <1.0 means the negative
	// cache absorbed some.
	GetAmplification float64 `json:"getAmplification"`
}

// Snapshot reads the current values.
func (c *Counters) Snapshot() Snapshot {
	s := Snapshot{
		BackendGet:        c.BackendGet.Load(),
		BackendList:       c.BackendList.Load(),
		ServedGet:         c.ServedGet.Load(),
		ServedList:        c.ServedList.Load(),
		AdoptInline:       c.AdoptInline.Load(),
		AdoptSweep:        c.AdoptSweep.Load(),
		CollectInline:     c.CollectInline.Load(),
		CollectSweep:      c.CollectSweep.Load(),
		CollectSkippedAge: c.CollectSkippedAge.Load(),
		NegCacheHit:       c.NegCacheHit.Load(),
		NegCacheMiss:      c.NegCacheMiss.Load(),
		WriteRetry:        c.WriteRetry.Load(),
		WriteOK:           c.WriteOK.Load(),
		WriteConflict:     c.WriteConflict.Load(),
		MaxWriteDepth:     c.MaxWriteDepth.Load(),
	}
	if s.ServedGet > 0 {
		s.GetAmplification = float64(s.BackendGet) / float64(s.ServedGet)
	}
	return s
}

// Reset zeroes every counter.
func (c *Counters) Reset() {
	c.BackendGet.Store(0)
	c.BackendList.Store(0)
	c.ServedGet.Store(0)
	c.ServedList.Store(0)
	c.AdoptInline.Store(0)
	c.AdoptSweep.Store(0)
	c.CollectInline.Store(0)
	c.CollectSweep.Store(0)
	c.CollectSkippedAge.Store(0)
	c.NegCacheHit.Store(0)
	c.NegCacheMiss.Store(0)
	c.WriteRetry.Store(0)
	c.WriteOK.Store(0)
	c.WriteConflict.Store(0)
	c.MaxWriteDepth.Store(0)
}
