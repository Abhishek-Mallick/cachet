package admission

import (
	"container/list"
	"sync"
	"time"
)

// ControllerOptions configures a Controller.
type ControllerOptions struct {
	Sketch *Sketch
	Policy Policy

	// StatesTracked bounds how many keys carry an admission state.
	//
	// The state is small but it is per key, so without a bound it grows with the key space — the
	// exact cost the sketch was chosen to avoid, reintroduced one layer up. A key that falls out of
	// the bound reverts to the default, which is the same thing that happens to a key nobody has
	// seen before.
	StatesTracked int

	Now func() time.Time
}

const defaultStatesTracked = 100_000

// Controller is the live admission decision for every key.
//
// It holds the small amount of state the policy needs that a sketch cannot provide: whether a key
// is currently admitted, and since when. Both are required for hysteresis — the decision depends on
// where a key IS and not only on its ratio, which is the entire mechanism that stops it flipping.
type Controller struct {
	sketch *Sketch
	policy Policy
	bound  int
	now    func() time.Time

	mu    sync.Mutex
	order *list.List // most recently decided first
	byKey map[string]*list.Element
}

type keyState struct {
	key      string
	admitted bool
	since    time.Time
}

// NewController builds a Controller.
func NewController(opts ControllerOptions) *Controller {
	if opts.Sketch == nil {
		opts.Sketch = NewSketch(SketchOptions{})
	}
	if opts.StatesTracked <= 0 {
		opts.StatesTracked = defaultStatesTracked
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	return &Controller{
		sketch: opts.Sketch,
		policy: opts.Policy,
		bound:  opts.StatesTracked,
		now:    opts.Now,
		order:  list.New(),
		byKey:  make(map[string]*list.Element, opts.StatesTracked),
	}
}

// RecordRead counts a read against a key's ratio.
func (c *Controller) RecordRead(key string) { c.sketch.RecordRead(key) }

// RecordWrite counts a write against a key's ratio.
func (c *Controller) RecordWrite(key string) { c.sketch.RecordWrite(key) }

// ShouldCache decides whether a key is worth caching, updating its state.
func (c *Controller) ShouldCache(key string) bool { return c.Explain(key).Admit }

// Explain decides, and returns the reasoning.
//
// The same code path as ShouldCache rather than a parallel one, so `cachetctl admission explain`
// can never describe a decision different from the one actually being made. Two implementations
// would eventually disagree, and the explanation is exactly the thing nobody would think to test
// against reality.
func (c *Controller) Explain(key string) Decision {
	reads, writes := c.sketch.Counts(key)
	now := c.now()

	c.mu.Lock()
	defer c.mu.Unlock()

	state := c.stateLocked(key, now)
	d := c.policy.Decide(State{
		Key: key, Reads: reads, Writes: writes,
		Admitted: state.admitted, Since: state.since, Now: now,
	})

	if d.Admit != state.admitted {
		state.admitted = d.Admit
		// The dwell clock restarts on every CHANGE, not on every decision. Restarting it on each
		// sample would mean a frequently-sampled key never completes its dwell and can never change
		// state at all.
		state.since = now
	}
	return d
}

// stateLocked returns a key's state, creating it if new.
func (c *Controller) stateLocked(key string, now time.Time) *keyState {
	if el, ok := c.byKey[key]; ok {
		c.order.MoveToFront(el)
		s, _ := el.Value.(*keyState)
		return s
	}

	if c.order.Len() >= c.bound {
		if back := c.order.Back(); back != nil {
			old, _ := back.Value.(*keyState)
			delete(c.byKey, old.key)
			c.order.Remove(back)
		}
	}

	// A new key starts in the policy's default state. Its Since is now, so it also serves its dwell
	// before it can be moved — which stops a burst of cold keys from being admitted and evicted in
	// quick succession while their ratios are still forming.
	s := &keyState{key: key, admitted: true, since: now}
	c.byKey[key] = c.order.PushFront(s)
	return s
}

// Counts returns a key's observed reads and writes over the window, for explanation and metrics.
func (c *Controller) Counts(key string) (reads, writes uint32) { return c.sketch.Counts(key) }

// TrackedKeys is how many keys carry an admission state.
func (c *Controller) TrackedKeys() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.order.Len()
}
