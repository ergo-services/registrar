package etcd

import (
	"fmt"
	"testing"
	"time"

	"ergo.services/ergo/gen"
	"ergo.services/ergo/testing/mock"
	"go.etcd.io/etcd/api/v3/mvccpb"
	etcdcli "go.etcd.io/etcd/client/v3"
)

// newMirrorTestClient builds a client with no etcd connection at all: every
// test here drives the mirror through watch responses, so a call tha.
func newMirrorTestClient(t *testing.T, name string) *client {
	t.Helper()

	cluster := "test"
	c := &client{
		options: Options{
			Cluster:       cluster,
			SuspectGrace:  3 * time.Second,
			SweepInterval: time.Second,
		},
		mirror:           newMirror(),
		config:           make(map[string]any),
		pathNodes:        fmt.Sprintf(formatPathNodes, cluster),
		pathApps:         fmt.Sprintf(formatPathApps, cluster),
		pathLeaving:      fmt.Sprintf(formatPathLeaving, cluster),
		pathConfig:       fmt.Sprintf(formatPathConfig, cluster),
		pathGlobalConfig: formatPathGlobalConfig,
	}
	c.node = newMockNode(t, name)
	return c
}

func nodeKeyEvent(t *testing.T, c *client, node gen.Atom, rev int64, put bool) *etcdcli.Event {
	t.Helper()

	kv := &mvccpb.KeyValue{Key: []byte(c.pathNodes + string(node)), ModRevision: rev}
	if put == false {
		return &etcdcli.Event{Type: etcdcli.EventTypeDelete, Kv: kv}
	}

	value, err := encode([]gen.Route{{Host: "localhost", Port: 9001}})
	if err != nil {
		t.Fatalf("encode node routes: %v", err)
	}
	kv.Value = []byte(value)
	return &etcdcli.Event{Type: etcdcli.EventTypePut, Kv: kv}
}

func appKeyEvent(t *testing.T, c *client, app, node gen.Atom, rev int64, put bool) *etcdcli.Event {
	t.Helper()

	kv := &mvccpb.KeyValue{Key: []byte(c.pathApps + string(app) + "/" + string(node)), ModRevision: rev}
	if put == false {
		return &etcdcli.Event{Type: etcdcli.EventTypeDelete, Kv: kv}
	}

	value, err := encode(gen.ApplicationRoute{
		Node:   node,
		Name:   app,
		Weight: 1,
		State:  gen.ApplicationStateRunning,
	})
	if err != nil {
		t.Fatalf("encode application route: %v", err)
	}
	kv.Value = []byte(value)
	return &etcdcli.Event{Type: etcdcli.EventTypePut, Kv: kv}
}

func routeSuspect(t *testing.T, c *client, app, node gen.Atom) (suspect bool, present bool) {
	t.Helper()

	c.mirror.lock.RLock()
	defer c.mirror.lock.RUnlock()

	entry, ok := c.mirror.apps[app]
	if ok == false {
		return false, false
	}
	if _, ok := entry.routes[node]; ok == false {
		return false, false
	}
	_, suspect = entry.suspect[node]
	return suspect, true
}

// TestMirrorLeaseExpiryIsInference: a lease expiry removes the node key and
// the application keys in one revision, and etcd gives no ordering inside it,
// so both admissible orders must classify the removal as an inference.
func TestMirrorLeaseExpiryIsInference(t *testing.T) {
	orders := []struct {
		name    string
		reverse bool
	}{
		{"node key first", false},
		{"application key first", true},
	}

	for _, order := range orders {
		t.Run(order.name, func(t *testing.T) {
			c := newMirrorTestClient(t, "self@h")

			c.applyWatchResponse(etcdcli.WatchResponse{Events: []*etcdcli.Event{
				nodeKeyEvent(t, c, "peer@h", 10, true),
				appKeyEvent(t, c, "myapp", "peer@h", 10, true),
			}})

			if suspect, present := routeSuspect(t, c, "myapp", "peer@h"); present == false || suspect {
				t.Fatalf("before expiry: present=%v suspect=%v, want present and healthy", present, suspect)
			}

			// One revision, both keys gone.
			expiry := []*etcdcli.Event{
				nodeKeyEvent(t, c, "peer@h", 20, false),
				appKeyEvent(t, c, "myapp", "peer@h", 20, false),
			}
			if order.reverse {
				expiry[0], expiry[1] = expiry[1], expiry[0]
			}
			c.applyWatchResponse(etcdcli.WatchResponse{Events: expiry})

			suspect, present := routeSuspect(t, c, "myapp", "peer@h")
			if present == false {
				t.Fatal("route dropped on lease expiry, it must survive under suspicion")
			}
			if suspect == false {
				t.Fatal("route kept as healthy after its owner disappeared")
			}
		})
	}
}

// TestMirrorOwnerRemovalIsIntent is the other half of the rule: while the node
// key is there, the owner is speaking for itself and the route goes at once.
func TestMirrorOwnerRemovalIsIntent(t *testing.T) {
	c := newMirrorTestClient(t, "self@h")

	c.applyWatchResponse(etcdcli.WatchResponse{Events: []*etcdcli.Event{
		nodeKeyEvent(t, c, "peer@h", 10, true),
		appKeyEvent(t, c, "myapp", "peer@h", 10, true),
	}})
	c.applyWatchResponse(etcdcli.WatchResponse{Events: []*etcdcli.Event{
		appKeyEvent(t, c, "myapp", "peer@h", 20, false),
	}})

	if _, present := routeSuspect(t, c, "myapp", "peer@h"); present {
		t.Fatal("intentional removal left the route in the mirror")
	}
}

func TestMirrorNodeReturnClearsSuspicion(t *testing.T) {
	c := newMirrorTestClient(t, "self@h")

	c.applyWatchResponse(etcdcli.WatchResponse{Events: []*etcdcli.Event{
		nodeKeyEvent(t, c, "peer@h", 10, true),
		appKeyEvent(t, c, "myapp", "peer@h", 10, true),
	}})
	c.applyWatchResponse(etcdcli.WatchResponse{Events: []*etcdcli.Event{
		nodeKeyEvent(t, c, "peer@h", 20, false),
	}})

	if suspect, _ := routeSuspect(t, c, "myapp", "peer@h"); suspect == false {
		t.Fatal("route not suspect after the node key vanished")
	}

	c.applyWatchResponse(etcdcli.WatchResponse{Events: []*etcdcli.Event{
		nodeKeyEvent(t, c, "peer@h", 30, true),
	}})

	suspect, present := routeSuspect(t, c, "myapp", "peer@h")
	if present == false {
		t.Fatal("route lost when the node came back")
	}
	if suspect {
		t.Fatal("suspicion survived the return of the node key")
	}
}

// TestMirrorSuspicionNotRestamped guards the grace against being extended
// forever by a second wave of deletions.
func TestMirrorSuspicionNotRestamped(t *testing.T) {
	c := newMirrorTestClient(t, "self@h")

	c.applyWatchResponse(etcdcli.WatchResponse{Events: []*etcdcli.Event{
		nodeKeyEvent(t, c, "peer@h", 10, true),
		appKeyEvent(t, c, "myapp", "peer@h", 10, true),
	}})
	c.applyWatchResponse(etcdcli.WatchResponse{Events: []*etcdcli.Event{
		nodeKeyEvent(t, c, "peer@h", 20, false),
	}})

	c.mirror.lock.RLock()
	first := c.mirror.apps["myapp"].suspect["peer@h"]
	c.mirror.lock.RUnlock()

	c.sweepMirror()

	// A late application delete for the same route must not restart the clock.
	c.applyWatchResponse(etcdcli.WatchResponse{Events: []*etcdcli.Event{
		appKeyEvent(t, c, "myapp", "peer@h", 30, false),
	}})

	c.mirror.lock.RLock()
	left := c.mirror.apps["myapp"].suspect["peer@h"]
	c.mirror.lock.RUnlock()

	if left != first-1 {
		t.Fatalf("grace restarted: %d ticks left, want %d", left, first-1)
	}
}

func TestMirrorSweepExpiresRouteAndNode(t *testing.T) {
	c := newMirrorTestClient(t, "self@h")

	c.applyWatchResponse(etcdcli.WatchResponse{Events: []*etcdcli.Event{
		nodeKeyEvent(t, c, "peer@h", 10, true),
		appKeyEvent(t, c, "myapp", "peer@h", 10, true),
	}})
	c.applyWatchResponse(etcdcli.WatchResponse{Events: []*etcdcli.Event{
		nodeKeyEvent(t, c, "peer@h", 20, false),
		appKeyEvent(t, c, "myapp", "peer@h", 20, false),
	}})

	c.mirror.lock.RLock()
	ticks := c.mirror.apps["myapp"].suspect["peer@h"]
	c.mirror.lock.RUnlock()
	if ticks < 1 {
		t.Fatalf("no grace assigned: %d ticks", ticks)
	}

	for i := 0; i < ticks-1; i++ {
		c.sweepMirror()
		if _, present := routeSuspect(t, c, "myapp", "peer@h"); present == false {
			t.Fatalf("route expired after %d of %d ticks", i+1, ticks)
		}
	}

	c.sweepMirror()

	if _, present := routeSuspect(t, c, "myapp", "peer@h"); present {
		t.Fatal("route survived its grace")
	}
	c.mirror.lock.RLock()
	_, nodeLeft := c.mirror.nodes["peer@h"]
	c.mirror.lock.RUnlock()
	if nodeLeft {
		t.Fatal("node survived its grace")
	}
}

// TestMirrorSuspicionFrozenWithoutSession: the sweep is the only clock, and it
// only runs from the watch loop, so a registrar with no session expires nothing.
func TestMirrorSuspicionFrozenWithoutSession(t *testing.T) {
	c := newMirrorTestClient(t, "self@h")

	c.applyWatchResponse(etcdcli.WatchResponse{Events: []*etcdcli.Event{
		nodeKeyEvent(t, c, "peer@h", 10, true),
		appKeyEvent(t, c, "myapp", "peer@h", 10, true),
		nodeKeyEvent(t, c, "peer@h", 20, false),
	}})

	c.mirror.lock.RLock()
	before := c.mirror.apps["myapp"].suspect["peer@h"]
	c.mirror.lock.RUnlock()

	time.Sleep(50 * time.Millisecond)

	c.mirror.lock.RLock()
	after := c.mirror.apps["myapp"].suspect["peer@h"]
	c.mirror.lock.RUnlock()

	if after != before {
		t.Fatalf("grace advanced without a sweep: %d -> %d", before, after)
	}
}

func TestResolveApplicationPrefersHealthyKeepsSuspectInTail(t *testing.T) {
	c := newMirrorTestClient(t, "self@h")

	c.applyWatchResponse(etcdcli.WatchResponse{Events: []*etcdcli.Event{
		nodeKeyEvent(t, c, "a@h", 10, true),
		nodeKeyEvent(t, c, "b@h", 10, true),
		appKeyEvent(t, c, "myapp", "a@h", 10, true),
		appKeyEvent(t, c, "myapp", "b@h", 10, true),
	}})
	c.applyWatchResponse(etcdcli.WatchResponse{Events: []*etcdcli.Event{
		nodeKeyEvent(t, c, "b@h", 20, false),
	}})

	for i := 0; i < 6; i++ {
		routes, err := c.ResolveApplication("myapp")
		if err != nil {
			t.Fatalf("resolve failed: %v", err)
		}
		if len(routes) != 2 {
			t.Fatalf("resolve returned %d routes, want 2", len(routes))
		}
		if routes[0].Node != "a@h" {
			t.Fatalf("suspect route won the rotation: %v", routes[0].Node)
		}
		if routes[1].Node != "b@h" {
			t.Fatalf("suspect route missing from the tail: %v", routes[1].Node)
		}
	}
}

func TestResolveApplicationServesAllSuspect(t *testing.T) {
	c := newMirrorTestClient(t, "self@h")

	c.applyWatchResponse(etcdcli.WatchResponse{Events: []*etcdcli.Event{
		nodeKeyEvent(t, c, "a@h", 10, true),
		nodeKeyEvent(t, c, "b@h", 10, true),
		appKeyEvent(t, c, "myapp", "a@h", 10, true),
		appKeyEvent(t, c, "myapp", "b@h", 10, true),
	}})
	// The whole cluster loses its leases at once, which is what an etcd
	// hiccup looks like from here. Resolve must keep working.
	c.applyWatchResponse(etcdcli.WatchResponse{Events: []*etcdcli.Event{
		nodeKeyEvent(t, c, "a@h", 20, false),
		nodeKeyEvent(t, c, "b@h", 20, false),
		appKeyEvent(t, c, "myapp", "a@h", 20, false),
		appKeyEvent(t, c, "myapp", "b@h", 20, false),
	}})

	counts := map[gen.Atom]int{}
	for i := 0; i < 4; i++ {
		routes, err := c.ResolveApplication("myapp")
		if err != nil {
			t.Fatalf("resolve failed with every route suspect: %v", err)
		}
		if len(routes) != 2 {
			t.Fatalf("resolve returned %d routes, want 2", len(routes))
		}
		counts[routes[0].Node]++
	}
	if counts["a@h"] != 2 || counts["b@h"] != 2 {
		t.Fatalf("suspect rotation not fair: %v", counts)
	}
}

// TestResolveApplicationSeededMissDoesNotDial proves the mirror answers on its
// own once seeded: the client has no etcd connection, so any RPC would panic.
func TestResolveApplicationSeededMissDoesNotDial(t *testing.T) {
	c := newMirrorTestClient(t, "self@h")
	c.mirror.reconcile(nil, nil, nil, 100, c.graceTicks)

	if _, err := c.ResolveApplication("nosuchapp"); err != gen.ErrNoRoute {
		t.Fatalf("resolve of an unknown application: got %v, want ErrNoRoute", err)
	}
}

func TestReconcileClassifiesMissingKeys(t *testing.T) {
	c := newMirrorTestClient(t, "self@h")

	// alive@h keeps its node key and retires one application while we were
	// not watching. gone@h lost everything.
	c.applyWatchResponse(etcdcli.WatchResponse{Events: []*etcdcli.Event{
		nodeKeyEvent(t, c, "alive@h", 10, true),
		nodeKeyEvent(t, c, "gone@h", 10, true),
		appKeyEvent(t, c, "myapp", "alive@h", 10, true),
		appKeyEvent(t, c, "myapp", "gone@h", 10, true),
	}})

	c.mirror.reconcile(
		map[gen.Atom][]gen.Route{"alive@h": {{Host: "localhost", Port: 9001}}},
		nil,
		nil,
		50,
		c.graceTicks,
	)

	if _, present := routeSuspect(t, c, "myapp", "alive@h"); present {
		t.Error("route of a live owner missing from the snapshot must be removed, not suspected")
	}
	suspect, present := routeSuspect(t, c, "myapp", "gone@h")
	if present == false {
		t.Error("route of a vanished owner dropped by reconcile instead of suspected")
	}
	if present && suspect == false {
		t.Error("route of a vanished owner kept as healthy")
	}
}

func TestGraceTicksJitterWithinBounds(t *testing.T) {
	c := newMirrorTestClient(t, "self@h")
	c.options.SuspectGrace = 30 * time.Second
	c.options.SweepInterval = time.Second

	spread := map[int]bool{}
	for i := 0; i < 200; i++ {
		ticks := c.graceTicks()
		if ticks < 24 || ticks > 36 {
			t.Fatalf("grace %d ticks outside the +/-20%% band of 30", ticks)
		}
		spread[ticks] = true
	}
	if len(spread) < 2 {
		t.Fatal("grace is not jittered, a fleet would expire in lockstep")
	}
}

func leavingKeyEvent(t *testing.T, c *client, node gen.Atom, rev int64, put bool) *etcdcli.Event {
	t.Helper()

	kv := &mvccpb.KeyValue{Key: []byte(c.pathLeaving + string(node)), ModRevision: rev}
	if put == false {
		return &etcdcli.Event{Type: etcdcli.EventTypeDelete, Kv: kv}
	}
	return &etcdcli.Event{Type: etcdcli.EventTypePut, Kv: kv}
}

// TestMirrorAnnouncedDepartureIsIntent is phase 3.
func TestMirrorAnnouncedDepartureIsIntent(t *testing.T) {
	c := newMirrorTestClient(t, "self@h")

	c.applyWatchResponse(etcdcli.WatchResponse{Events: []*etcdcli.Event{
		nodeKeyEvent(t, c, "peer@h", 10, true),
		appKeyEvent(t, c, "myapp", "peer@h", 10, true),
	}})

	// Marker first, then the same one-revision removal a lease expiry makes.
	c.applyWatchResponse(etcdcli.WatchResponse{Events: []*etcdcli.Event{
		leavingKeyEvent(t, c, "peer@h", 20, true),
	}})
	c.applyWatchResponse(etcdcli.WatchResponse{Events: []*etcdcli.Event{
		appKeyEvent(t, c, "myapp", "peer@h", 21, false),
		nodeKeyEvent(t, c, "peer@h", 21, false),
	}})

	if _, present := routeSuspect(t, c, "myapp", "peer@h"); present {
		t.Fatal("announced departure left the route under suspicion")
	}
	c.mirror.lock.RLock()
	_, nodePresent := c.mirror.nodes["peer@h"]
	c.mirror.lock.RUnlock()
	if nodePresent {
		t.Fatal("announced departure left the node in the mirror")
	}
}

// TestMirrorDepartureMarkerInSameRevision: the marker may arrive in the very
// revision that removes the keys, which is what one shutdown transaction does.
func TestMirrorDepartureMarkerInSameRevision(t *testing.T) {
	c := newMirrorTestClient(t, "self@h")

	c.applyWatchResponse(etcdcli.WatchResponse{Events: []*etcdcli.Event{
		nodeKeyEvent(t, c, "peer@h", 10, true),
		appKeyEvent(t, c, "myapp", "peer@h", 10, true),
	}})
	c.applyWatchResponse(etcdcli.WatchResponse{Events: []*etcdcli.Event{
		appKeyEvent(t, c, "myapp", "peer@h", 20, false),
		nodeKeyEvent(t, c, "peer@h", 20, false),
		leavingKeyEvent(t, c, "peer@h", 20, true),
	}})

	if _, present := routeSuspect(t, c, "myapp", "peer@h"); present {
		t.Fatal("departure announced in the same revision was not honoured")
	}
}

// TestMirrorDepartureMarkerExpires: the marker carries a short lease of its own,
// and its disappearance must not be read as anything about the node.
func TestMirrorDepartureMarkerExpires(t *testing.T) {
	c := newMirrorTestClient(t, "self@h")

	c.applyWatchResponse(etcdcli.WatchResponse{Events: []*etcdcli.Event{
		leavingKeyEvent(t, c, "peer@h", 10, true),
	}})
	c.mirror.lock.RLock()
	announced := c.mirror.isLeaving("peer@h")
	c.mirror.lock.RUnlock()
	if announced == false {
		t.Fatal("departure marker not recorded")
	}

	c.applyWatchResponse(etcdcli.WatchResponse{Events: []*etcdcli.Event{
		leavingKeyEvent(t, c, "peer@h", 20, false),
	}})
	c.mirror.lock.RLock()
	announced = c.mirror.isLeaving("peer@h")
	c.mirror.lock.RUnlock()
	if announced {
		t.Fatal("expired departure marker still counts as an announcement")
	}
}

func TestReconcileHonoursDepartureMarker(t *testing.T) {
	// A node that announced its departure and then vanished from the snapshot
	// left on purpose, even though we were not watching when it happened.
	c := newMirrorTestClient(t, "self@h")

	c.applyWatchResponse(etcdcli.WatchResponse{Events: []*etcdcli.Event{
		nodeKeyEvent(t, c, "peer@h", 10, true),
		appKeyEvent(t, c, "myapp", "peer@h", 10, true),
	}})

	c.mirror.reconcile(
		nil,
		nil,
		map[gen.Atom]struct{}{"peer@h": {}},
		50,
		c.graceTicks,
	)

	if _, present := routeSuspect(t, c, "myapp", "peer@h"); present {
		t.Error("an announced departure was put under suspicion by the resync")
	}
	c.mirror.lock.RLock()
	_, nodePresent := c.mirror.nodes["peer@h"]
	c.mirror.lock.RUnlock()
	if nodePresent {
		t.Error("an announced departure left the node in the mirror after the resync")
	}
}

// TestMirrorIntentBeatsSuspicion pins the precedence rule: whatever the owner
// says wins over anything we merely inferred, including in the same revision.
func TestMirrorIntentBeatsSuspicion(t *testing.T) {
	c := newMirrorTestClient(t, "self@h")

	c.applyWatchResponse(etcdcli.WatchResponse{Events: []*etcdcli.Event{
		nodeKeyEvent(t, c, "peer@h", 10, true),
		appKeyEvent(t, c, "myapp", "peer@h", 10, true),
	}})
	c.applyWatchResponse(etcdcli.WatchResponse{Events: []*etcdcli.Event{
		nodeKeyEvent(t, c, "peer@h", 20, false),
	}})
	if suspect, _ := routeSuspect(t, c, "myapp", "peer@h"); suspect == false {
		t.Fatal("route not suspect after its owner disappeared")
	}

	// The owner is back and retires the route in the same revision.
	c.applyWatchResponse(etcdcli.WatchResponse{Events: []*etcdcli.Event{
		nodeKeyEvent(t, c, "peer@h", 30, true),
		appKeyEvent(t, c, "myapp", "peer@h", 30, false),
	}})

	if _, present := routeSuspect(t, c, "myapp", "peer@h"); present {
		t.Fatal("an intentional removal was held back by an older suspicion")
	}
}

// TestSweepAnnouncesExpiredLoss: nothing is published while the suspicion
// holds, both losses are published on the tick it expires.
func TestSweepAnnouncesExpiredLoss(t *testing.T) {
	c := newMirrorTestClient(t, "self@h")
	node := c.node.(*mock.Node)

	c.applyWatchResponse(etcdcli.WatchResponse{Events: []*etcdcli.Event{
		nodeKeyEvent(t, c, "peer@h", 10, true),
		appKeyEvent(t, c, "myapp", "peer@h", 10, true),
	}})

	mark := node.Mark()

	c.applyWatchResponse(etcdcli.WatchResponse{Events: []*etcdcli.Event{
		nodeKeyEvent(t, c, "peer@h", 20, false),
		appKeyEvent(t, c, "myapp", "peer@h", 20, false),
	}})

	c.mirror.lock.RLock()
	ticks := c.mirror.apps["myapp"].suspect["peer@h"]
	c.mirror.lock.RUnlock()

	for i := 0; i < ticks-1; i++ {
		c.sweepMirror()
	}
	node.ShouldSendEvent().Where(eventType[EventApplicationStopped]()).Since(mark).None().Assert()
	node.ShouldSendEvent().Where(eventType[EventNodeLeft]()).Since(mark).None().Assert()

	c.sweepMirror()

	node.ShouldSendEvent().
		Where(eventMessage(EventApplicationStopped{Name: "myapp", Node: "peer@h"})).
		Since(mark).
		Once().
		Assert()
	node.ShouldSendEvent().
		Where(eventMessage(EventNodeLeft{Name: "peer@h"})).
		Since(mark).
		Once().
		Assert()
}

// TestMirrorIgnoresUnknownRoutesKey: a key under the routes prefix that this
// version does not know must be ignored, not decoded and not applied.
func TestMirrorIgnoresUnknownRoutesKey(t *testing.T) {
	c := newMirrorTestClient(t, "self@h")

	c.applyWatchResponse(etcdcli.WatchResponse{Events: []*etcdcli.Event{
		nodeKeyEvent(t, c, "peer@h", 10, true),
		appKeyEvent(t, c, "myapp", "peer@h", 10, true),
		{
			Type: etcdcli.EventTypePut,
			Kv: &mvccpb.KeyValue{
				Key:         []byte(fmt.Sprintf(formatPathClusterRoutes, "test") + "something/new@h"),
				Value:       []byte("whatever a future version writes"),
				ModRevision: 10,
			},
		},
	}})

	if suspect, present := routeSuspect(t, c, "myapp", "peer@h"); present == false || suspect {
		t.Fatalf("an unknown key disturbed the mirror: present=%v suspect=%v", present, suspect)
	}
	c.mirror.lock.RLock()
	nodes := len(c.mirror.nodes)
	c.mirror.lock.RUnlock()
	if nodes != 1 {
		t.Fatalf("an unknown key added %d node(s) to the mirror", nodes-1)
	}
}
