package live

import (
	"sync"
	"time"
)

// session_cache pretent Aggregator periodically implement ResolveID function and,
// read the database per mintue
// RoomID -> SessionID in cache
// L3 level storage L1 cache,L2 redis, L3 mysql

type sessionCache struct {
	mu sync.RWMutex

	// RoomID->SessionEntry
	RSmap map[int64]sessionEntry
}

// SessionID = 0 equals the room is offline.
type sessionEntry struct {
	SessionID int64
	expiredAt time.Time
}

const (
	sessionCachePositiveTTL = 30 * time.Second
	sessionCacheNegativeTTL = 3 * time.Second
)

func newSessionCache() *sessionCache {
	return &sessionCache{RSmap: make(map[int64]sessionEntry)}
}

func (c *sessionCache) get(roomID int64, now time.Time) (int64, bool) {
	c.mu.RLock()
	v, ok := c.RSmap[roomID]
	c.mu.RUnlock()

	if !ok || now.After(v.expiredAt) {
		return 0, false
	}

	return v.SessionID, true
}

func (c *sessionCache) set(roomID, sessionID int64, now time.Time) {
	ttl := sessionCachePositiveTTL
	
	if sessionID == 0 {
		ttl = sessionCacheNegativeTTL
	}

	c.mu.Lock()
	c.RSmap[roomID] = sessionEntry{
		SessionID: sessionID,
		expiredAt: now.Add(ttl),
	}
	c.mu.Unlock()
}


// Proactive delete 
func (c *sessionCache) del(roomID int64) {
	c.mu.Lock()
	delete(c.RSmap, roomID)
	c.mu.Unlock()
}


// evict all expired Cache
func (c *sessionCache) evictSCache(now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for roomId, e := range c.RSmap {
		if now.After(e.expiredAt) {
			delete(c.RSmap, roomId)
		}
	}
}
