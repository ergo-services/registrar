package etcd

import (
	"context"
	"sort"

	"ergo.services/ergo/gen"
	etcdcli "go.etcd.io/etcd/client/v3"
)

//
// gen.Resolver interface implementation
//

func (c *client) Resolve(name gen.Atom) ([]gen.Route, error) {
	// Use exact key lookup for the specific node
	key := c.pathNodes + string(name)

	// Create timeout context for etcd operation
	ctx, cancel := context.WithTimeout(context.Background(), c.options.RequestTimeout)
	defer cancel()

	resp, err := c.cli.Get(ctx, key)
	if err != nil {
		return nil, err
	}

	if resp.Count == 0 {
		return nil, gen.ErrNoRoute
	}

	v, err := decode(resp.Kvs[0].Value)
	if err != nil {
		return nil, err
	}

	routes, ok := v.([]gen.Route)
	if !ok {
		c.node.Log().Error("(registrar) invalid type for node route %s: %T", key, v)
		return nil, gen.ErrNoRoute
	}

	return routes, nil
}

func (c *client) ResolveApplication(name gen.Atom) (gen.ApplicationRoutes, error) {
	// Fast path: serve from cache if populated (rev > 0 means initial Get
	// completed and the entry is tracking Watch updates).
	c.appCacheLock.RLock()
	if entry, ok := c.appCache[name]; ok && entry.rev > 0 {
		snap := snapshotAppRoutes(entry)
		rrGen := entry.rrGen
		c.appCacheLock.RUnlock()
		if len(snap) == 0 {
			return nil, gen.ErrNoRoute
		}
		return rotateAppRoutes(entry, snap, rrGen), nil
	}
	c.appCacheLock.RUnlock()

	// Cache miss. Install a placeholder BEFORE issuing Get so that any
	// Watch event arriving during the Get has a target entry to update.
	// Otherwise such events would be dropped and the Get could overwrite
	// a newer state with stale data.
	c.appCacheLock.Lock()
	entry, ok := c.appCache[name]
	if ok == false {
		entry = &appEntry{routes: make(map[gen.Atom]gen.ApplicationRoute)}
		c.appCache[name] = entry
	}
	c.appCacheLock.Unlock()

	key := c.pathApps + string(name)
	ctx, cancel := context.WithTimeout(context.Background(), c.options.RequestTimeout)
	defer cancel()

	resp, err := c.cli.Get(ctx, key, etcdcli.WithPrefix())
	if err != nil {
		// Drop placeholder so a subsequent resolve can retry the Get.
		// Only drop if still unpopulated — a concurrent Watch event may
		// have legitimately filled the entry while Get was in flight.
		c.appCacheLock.Lock()
		if cur, ok := c.appCache[name]; ok && cur == entry && cur.rev == 0 {
			delete(c.appCache, name)
		}
		c.appCacheLock.Unlock()
		return nil, err
	}

	getRev := resp.Header.Revision

	c.appCacheLock.Lock()
	if entry.rev < getRev {
		// No newer Watch event has touched this entry — Get result wins.
		routes := make(map[gen.Atom]gen.ApplicationRoute, len(resp.Kvs))
		for _, kv := range resp.Kvs {
			v, decErr := decode(kv.Value)
			if decErr != nil {
				c.node.Log().Error("(registrar) failed to decode application route for %s: %v", kv.Key, decErr)
				continue
			}
			route, rok := v.(gen.ApplicationRoute)
			if rok == false {
				c.node.Log().Error("(registrar) invalid type for application route %s: %T", kv.Key, v)
				continue
			}
			routes[route.Node] = route
		}
		entry.routes = routes
		entry.rev = getRev
		entry.rrGen++
	}
	// else: a Watch event with rev >= getRev already updated the entry;
	// Get result is stale relative to that event — keep the cache as is.
	snap := snapshotAppRoutes(entry)
	rrGen := entry.rrGen
	c.appCacheLock.Unlock()

	if len(snap) == 0 {
		return nil, gen.ErrNoRoute
	}
	return rotateAppRoutes(entry, snap, rrGen), nil
}

// snapshotAppRoutes copies entry.routes into a stable, name-sorted slice.
// Sorting gives WRR a deterministic participant order so tie-breaks between
// equal-weight nodes are reproducible across calls. Caller must hold
// appCacheLock (read or write).
func snapshotAppRoutes(entry *appEntry) []gen.ApplicationRoute {
	out := make([]gen.ApplicationRoute, 0, len(entry.routes))
	for _, r := range entry.routes {
		if r.Weight < 0 {
			continue // negative weight opts the route out of resolve results
		}
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Node < out[j].Node })
	return out
}

// rotateAppRoutes selects a winner via smooth weighted round-robin and
// returns participants with the winner in position [0]. The tail is sorted
// by remaining current_weight (descending), with node name as tiebreaker —
// so callers using the slice for fallback see the next-most-preferred node
// next. participants must be non-empty.
func rotateAppRoutes(entry *appEntry, participants []gen.ApplicationRoute, rrGen uint64) gen.ApplicationRoutes {
	if len(participants) == 1 {
		return gen.ApplicationRoutes(participants)
	}
	winnerIdx, cw := wrrStep(entry, participants, rrGen)

	result := make(gen.ApplicationRoutes, 0, len(participants))
	result = append(result, participants[winnerIdx])
	for i, p := range participants {
		if i == winnerIdx {
			continue
		}
		result = append(result, p)
	}
	tail := result[1:]
	sort.SliceStable(tail, func(i, j int) bool {
		if cw[tail[i].Node] != cw[tail[j].Node] {
			return cw[tail[i].Node] > cw[tail[j].Node]
		}
		return tail[i].Node < tail[j].Node
	})
	return result
}

// wrrStep advances smooth weighted round-robin (Nginx-style) state and
// returns the index of the chosen participant together with a snapshot of
// post-step current_weight keyed by node name. Rebuilds rrState when the
// caller's snapshot is newer than what rrState was last reconciled against.
// Weights <= 0 are normalized to 1 so a forgotten field never excludes a
// node from rotation.
func wrrStep(entry *appEntry, participants []gen.ApplicationRoute, rrGen uint64) (int, map[gen.Atom]int) {
	entry.rrLock.Lock()
	defer entry.rrLock.Unlock()

	if entry.rrState == nil || entry.rrSeen < rrGen {
		entry.rrState = make(map[gen.Atom]int, len(participants))
		entry.rrSeen = rrGen
	}

	total := 0
	winnerIdx := 0
	winnerCW := 0
	for i, p := range participants {
		w := p.Weight
		if w <= 0 {
			w = 1
		}
		total += w
		cw := entry.rrState[p.Node] + w
		entry.rrState[p.Node] = cw
		if i == 0 || cw > winnerCW {
			winnerCW = cw
			winnerIdx = i
		}
	}

	entry.rrState[participants[winnerIdx].Node] -= total

	cwSnap := make(map[gen.Atom]int, len(participants))
	for _, p := range participants {
		cwSnap[p.Node] = entry.rrState[p.Node]
	}
	return winnerIdx, cwSnap
}

func (c *client) ResolveProxy(name gen.Atom) ([]gen.ProxyRoute, error) {
	// Proxy routing is not supported in etcd registrar implementation
	return nil, gen.ErrNoRoute
}
