package etcd

import (
	"math/rand"
	"sort"
	"sync"

	"ergo.services/ergo/gen"
)

// mirror is the full in-memory view of the cluster, seeded from etcd and kept
// current by the watch. A vanished key is only forgotten when its owner said so
// (node key still alive); anything else is an inference and the route stays
// resolvable under suspicion until the grace runs out.
type mirror struct {
	lock  sync.RWMutex
	nodes map[gen.Atom]*nodeEntry
	apps  map[gen.Atom]*appEntry

	leaving map[gen.Atom]struct{} // announced a deliberate shutdown, see departGracefully

	rev    int64 // revision the content is consistent to
	seeded bool  // a snapshot has been loaded at least once
}

type nodeEntry struct {
	routes  []gen.Route
	suspect int // remaining grace ticks, 0 means live
}

func newMirror() *mirror {
	return &mirror{
		nodes:   make(map[gen.Atom]*nodeEntry),
		apps:    make(map[gen.Atom]*appEntry),
		leaving: make(map[gen.Atom]struct{}),
	}
}

// Caller must hold m.lock.
func (m *mirror) setLeaving(name gen.Atom, leaving bool) {
	if leaving {
		m.leaving[name] = struct{}{}
		return
	}
	delete(m.leaving, name)
}

// Caller must hold m.lock.
func (m *mirror) isLeaving(name gen.Atom) bool {
	_, ok := m.leaving[name]
	return ok
}

// removeNode drops a node and everything it owns without grace.
// Caller must hold m.lock.
func (m *mirror) removeNode(name gen.Atom) {
	delete(m.nodes, name)

	for app, entry := range m.apps {
		if _, owns := entry.routes[name]; owns == false {
			continue
		}
		delete(entry.routes, name)
		delete(entry.suspect, name)
		entry.rrGen++
		if len(entry.routes) == 0 {
			delete(m.apps, app)
		}
	}
}

// nodeLive is the only admissible evidence of intent: a fact from etcd, never
// our own inference.
// Caller must hold m.lock.
func (m *mirror) nodeLive(name gen.Atom) bool {
	n, ok := m.nodes[name]
	return ok && n.suspect == 0
}

// putNode applies an observed node key and clears suspicion it caused.
// Caller must hold m.lock.
func (m *mirror) putNode(name gen.Atom, routes []gen.Route) {
	n, ok := m.nodes[name]
	if ok == false {
		n = &nodeEntry{}
		m.nodes[name] = n
	}
	n.routes = routes
	n.suspect = 0

	for _, entry := range m.apps {
		delete(entry.suspect, name)
	}
}

// suspectNode puts the node and everything it owns under suspicion.
// Caller must hold m.lock.
func (m *mirror) suspectNode(name gen.Atom, ticks int) {
	n, ok := m.nodes[name]
	if ok == false {
		return
	}
	if n.suspect == 0 {
		n.suspect = ticks
	}

	for _, entry := range m.apps {
		if _, owns := entry.routes[name]; owns == false {
			continue
		}
		entry.markSuspect(name, ticks)
	}
}

// putAppRoute records a route. rev is the observed ModRevision, or 0 to skip
// the staleness check (snapshots).
// Caller must hold m.lock.
func (m *mirror) putAppRoute(app, node gen.Atom, route gen.ApplicationRoute, rev int64) {
	entry, ok := m.apps[app]
	if ok == false {
		entry = &appEntry{routes: make(map[gen.Atom]gen.ApplicationRoute)}
		m.apps[app] = entry
	}
	if rev > 0 && rev < entry.rev {
		return
	}
	if _, existed := entry.routes[node]; existed == false {
		entry.rrGen++
	}
	entry.routes[node] = route
	delete(entry.suspect, node)
	if rev > entry.rev {
		entry.rev = rev
	}
}

// removeAppRoute drops a route for good.
// Caller must hold m.lock.
func (m *mirror) removeAppRoute(app, node gen.Atom, rev int64) {
	entry, ok := m.apps[app]
	if ok == false {
		return
	}
	if rev > 0 && rev < entry.rev {
		return
	}
	if _, existed := entry.routes[node]; existed {
		delete(entry.routes, node)
		entry.rrGen++
	}
	delete(entry.suspect, node)
	if rev > entry.rev {
		entry.rev = rev
	}
	if len(entry.routes) == 0 {
		delete(m.apps, app)
	}
}

// Caller must hold m.lock.
func (m *mirror) suspectAppRoute(app, node gen.Atom, ticks int, rev int64) {
	entry, ok := m.apps[app]
	if ok == false {
		return
	}
	if rev > 0 && rev < entry.rev {
		return
	}
	entry.markSuspect(node, ticks)
	if rev > entry.rev {
		entry.rev = rev
	}
}

// markSuspect never restamps: a second wave of deletions must not extend the
// grace of a route that is already suspect.
func (e *appEntry) markSuspect(node gen.Atom, ticks int) {
	if _, owns := e.routes[node]; owns == false {
		return
	}
	if e.suspect == nil {
		e.suspect = make(map[gen.Atom]int)
	}
	if _, already := e.suspect[node]; already {
		return
	}
	e.suspect[node] = ticks
}

// reconcile adopts a snapshot taken at rev, applying the same intent/inference
// rule to everything that vanished while we were not watching.
func (m *mirror) reconcile(
	nodes map[gen.Atom][]gen.Route,
	apps map[gen.Atom]map[gen.Atom]gen.ApplicationRoute,
	leaving map[gen.Atom]struct{},
	rev int64,
	ticks func() int,
) {
	m.lock.Lock()
	defer m.lock.Unlock()

	m.leaving = leaving
	if m.leaving == nil {
		m.leaving = make(map[gen.Atom]struct{})
	}

	for name, routes := range nodes {
		m.putNode(name, routes)
	}
	for name := range m.nodes {
		if _, present := nodes[name]; present {
			continue
		}
		if m.isLeaving(name) {
			m.removeNode(name)
			continue
		}
		m.suspectNode(name, ticks())
	}

	for app, byNode := range apps {
		for node, route := range byNode {
			m.putAppRoute(app, node, route, 0)
		}
	}
	for app, entry := range m.apps {
		for node := range entry.routes {
			if _, present := apps[app][node]; present {
				continue
			}
			if _, alive := nodes[node]; alive {
				m.removeAppRoute(app, node, 0)
				continue
			}
			entry.markSuspect(node, ticks())
		}
	}

	m.rev = rev
	m.seeded = true
}

type expiredRoute struct {
	app  gen.Atom
	node gen.Atom
}

// sweep advances the grace clock by one tick and drops whatever ran out. Only
// called while the watch is up, so a disconnected registrar expires nothing.
// What it returns is what the cluster still has to be told about.
func (m *mirror) sweep() (routes []expiredRoute, nodes []gen.Atom) {
	m.lock.Lock()
	defer m.lock.Unlock()

	for app, entry := range m.apps {
		for node, left := range entry.suspect {
			left--
			if left > 0 {
				entry.suspect[node] = left
				continue
			}
			delete(entry.suspect, node)
			if _, existed := entry.routes[node]; existed {
				delete(entry.routes, node)
				entry.rrGen++
				routes = append(routes, expiredRoute{app: app, node: node})
			}
		}
		if len(entry.routes) == 0 {
			delete(m.apps, app)
		}
	}

	for name, n := range m.nodes {
		if n.suspect == 0 {
			continue
		}
		n.suspect--
		if n.suspect == 0 {
			delete(m.nodes, name)
			nodes = append(nodes, name)
		}
	}

	return routes, nodes
}

func (m *mirror) suspectCount() int {
	m.lock.RLock()
	defer m.lock.RUnlock()

	count := 0
	for _, entry := range m.apps {
		count += len(entry.suspect)
	}
	return count
}

// splitAppRoutes returns healthy and suspect participants, each sorted by node
// name so rotation tie-breaks stay reproducible. Negative weight stays excluded.
// Caller must hold m.lock.
func splitAppRoutes(entry *appEntry) (healthy []gen.ApplicationRoute, suspect []gen.ApplicationRoute) {
	for node, route := range entry.routes {
		if route.Weight < 0 {
			continue
		}
		if _, ok := entry.suspect[node]; ok {
			suspect = append(suspect, route)
			continue
		}
		healthy = append(healthy, route)
	}
	sort.Slice(healthy, func(i, j int) bool { return healthy[i].Node < healthy[j].Node })
	sort.Slice(suspect, func(i, j int) bool { return suspect[i].Node < suspect[j].Node })
	return healthy, suspect
}

// graceTicks jitters the suspicion lifetime by up to 20% so a fleet that lost
// etcd at the same moment does not expire in lockstep.
func (c *client) graceTicks() int {
	ticks := int(c.options.SuspectGrace / c.options.SweepInterval)
	if ticks < 1 {
		ticks = 1
	}
	if jitter := ticks / 5; jitter > 0 {
		ticks += rand.Intn(2*jitter+1) - jitter
	}
	if ticks < 1 {
		ticks = 1
	}
	return ticks
}
