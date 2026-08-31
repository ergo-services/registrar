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
	// Suspects are served too: a lapsed registration on a running process is
	// exactly the case a new connection must still resolve.
	c.mirror.lock.RLock()
	if entry, ok := c.mirror.nodes[name]; ok && len(entry.routes) > 0 {
		routes := make([]gen.Route, len(entry.routes))
		copy(routes, entry.routes)
		c.mirror.lock.RUnlock()
		return routes, nil
	}
	c.mirror.lock.RUnlock()

	// Miss: never answer ErrNoRoute out of the mirror without asking etcd.
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
	// Never blocks, never issues an RPC: callers route on this per request.
	c.mirror.lock.RLock()
	entry, ok := c.mirror.apps[name]
	seeded := c.mirror.seeded
	var healthy, suspect []gen.ApplicationRoute
	var rrGen uint64
	if ok {
		healthy, suspect = splitAppRoutes(entry)
		rrGen = entry.rrGen
	}
	c.mirror.lock.RUnlock()

	if len(healthy) > 0 {
		// Suspects stay as a last resort, behind every healthy route.
		return append(rotateAppRoutes(entry, healthy, rrGen), suspect...), nil
	}

	if len(suspect) > 0 {
		// Everything is suspect, which is what a cluster-wide etcd hiccup looks
		// like. Serve it and say so: a silently absorbed failure is worse.
		c.statResolvedFromSuspect.Add(1)
		c.node.Log().Warning("(registrar) resolving %s from %d suspect route(s), no healthy route known",
			name, len(suspect))
		return rotateAppRoutes(entry, suspect, rrGen), nil
	}

	if seeded {
		// The mirror holds every application, so an empty answer is an answer.
		return nil, gen.ErrNoRoute
	}

	// The very first resolve can race the initial snapshot.
	key := c.pathApps + string(name)
	ctx, cancel := context.WithTimeout(context.Background(), c.options.RequestTimeout)
	defer cancel()

	resp, err := c.cli.Get(ctx, key, etcdcli.WithPrefix())
	if err != nil {
		return nil, err
	}

	c.mirror.lock.Lock()
	for _, kv := range resp.Kvs {
		appName, nodeName, ok := c.splitApplicationKey(string(kv.Key))
		if ok == false {
			continue
		}
		route, decErr := decodeApplicationRoute(kv.Value)
		if decErr != nil {
			c.node.Log().Error("(registrar) failed to decode application route %s: %v", kv.Key, decErr)
			continue
		}
		c.mirror.putAppRoute(appName, nodeName, route, kv.ModRevision)
	}
	// Read back under the same lock: a concurrent sweep may drop the entry.
	entry, ok = c.mirror.apps[name]
	if ok {
		healthy, suspect = splitAppRoutes(entry)
		rrGen = entry.rrGen
	}
	c.mirror.lock.Unlock()

	if len(healthy) == 0 && len(suspect) == 0 {
		return nil, gen.ErrNoRoute
	}
	if len(healthy) == 0 {
		return rotateAppRoutes(entry, suspect, rrGen), nil
	}
	return append(rotateAppRoutes(entry, healthy, rrGen), suspect...), nil
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
