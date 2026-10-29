package ownership

import (
	"container/list"
	"sync"
	"time"
)

// cache holds ownership the api has confirmed, and nothing else. A negative
// answer is never kept: an id that was not owned a moment ago may have been
// bound since on another gateway instance, and remembering "absent" would
// refuse the owner's first poll after a submit that landed elsewhere.
//
// The entry list is ordered by insertion, and an insert of an existing entry
// moves it to the back, so with one lifetime for every entry the front is
// always the entry that expires first.
type cache struct {
	mu      sync.Mutex
	ttl     time.Duration
	max     int
	now     func() time.Time
	order   *list.List
	entries map[cacheKey]*list.Element
}

type cacheKey struct {
	keyID    string
	kind     Kind
	objectID string
}

type cacheEntry struct {
	key     cacheKey
	expires time.Time
}

func newCache(ttl time.Duration, max int, now func() time.Time) *cache {
	return &cache{
		ttl:     ttl,
		max:     max,
		now:     now,
		order:   list.New(),
		entries: make(map[cacheKey]*list.Element),
	}
}

// owned reports whether the entry is present and unexpired.
func (c *cache) owned(k cacheKey) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	el, ok := c.entries[k]
	if !ok {
		return false
	}
	if !c.now().Before(el.Value.(*cacheEntry).expires) {
		c.order.Remove(el)
		delete(c.entries, k)
		return false
	}
	return true
}

func (c *cache) put(k cacheKey) {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.now()
	if el, ok := c.entries[k]; ok {
		el.Value.(*cacheEntry).expires = now.Add(c.ttl)
		c.order.MoveToBack(el)
		return
	}
	for c.order.Len() > 0 {
		front := c.order.Front()
		e := front.Value.(*cacheEntry)
		if c.order.Len() < c.max && now.Before(e.expires) {
			break
		}
		c.order.Remove(front)
		delete(c.entries, e.key)
	}
	c.entries[k] = c.order.PushBack(&cacheEntry{key: k, expires: now.Add(c.ttl)})
}

func (c *cache) forget(k cacheKey) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.entries[k]; ok {
		c.order.Remove(el)
		delete(c.entries, k)
	}
}

func (c *cache) len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.order.Len()
}
