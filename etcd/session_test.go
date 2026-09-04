package etcd

import (
	"testing"
	"time"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	etcdcli "go.etcd.io/etcd/client/v3"
)

func TestWatchNextRevFollowsLastEvent(t *testing.T) {
	// A response carrying events must resume past the last event, never past
	// the header: the store revision can be ahead of what this watch was told
	// about, and everything in between would be skipped.
	resp := etcdcli.WatchResponse{
		Header: &etcdserverpb.ResponseHeader{Revision: 100},
		Events: []*etcdcli.Event{
			{Type: etcdcli.EventTypePut, Kv: &mvccpb.KeyValue{ModRevision: 41}},
			{Type: etcdcli.EventTypePut, Kv: &mvccpb.KeyValue{ModRevision: 42}},
		},
	}

	if got := watchNextRev(resp, 1); got != 43 {
		t.Fatalf("next revision after events: got %d, want 43", got)
	}
}

func TestWatchNextRevUsesHeaderWithoutEvents(t *testing.T) {
	// A progress notification carries no events, and its header is exactly what
	// keeps the resume point moving while the cluster is quiet.
	resp := etcdcli.WatchResponse{Header: &etcdserverpb.ResponseHeader{Revision: 77}}

	if got := watchNextRev(resp, 10); got != 78 {
		t.Fatalf("next revision on progress notify: got %d, want 78", got)
	}

	if got := watchNextRev(etcdcli.WatchResponse{}, 10); got != 10 {
		t.Fatalf("next revision on an empty response: got %d, want the current 10", got)
	}
}

// TestIntegrationReviveLeaseKeepsEverything covers the decision phase 2 turns
// on: a closed keepalive channel is a question, not a verdict.
func TestIntegrationReviveLeaseKeepsEverything(t *testing.T) {
	endpoints := getTestEndpoints()
	if endpoints == nil {
		t.Skip("Skipping integration test - no ETCD_ENDPOINTS set")
	}

	const cluster = "session-revive-cluster"

	observer, observerNode := registerNode(t, Options{
		Endpoints: endpoints,
		Cluster:   cluster,
	}, "observer@h")

	// A keepalive entry that already exists is renewed on schedule, every
	// TTL/3, so a short TTL keeps this test quick. On the real path the entry
	// is gone by the time we re-arm and the first request goes out at once.
	victim, victimNode := registerNode(t, Options{
		Endpoints: endpoints,
		Cluster:   cluster,
		LeaseTTL:  6,
	}, "victim@h")

	if err := victim.RegisterApplicationRoute(appRoute(victimNode.Name(), "victim-app", 10)); err != nil {
		t.Fatalf("failed to publish the application route: %v", err)
	}

	waitFor(t, 10*time.Second, func() bool {
		present, suspect := routeState(observer, "victim-app", victimNode.Name())
		return present && suspect == false
	}, "observer never saw the route as healthy")

	leaseBefore := victim.leaseID()
	appKey := victim.pathApps + "victim-app/" + string(victimNode.Name())
	createBefore, _, found := appKeyInfo(t, victim, appKey)
	if found == false {
		t.Fatal("application route missing before the revival")
	}

	mark := observerNode.Mark()

	revived, ch := victim.reviveLease(victim.ctx, leaseBefore)
	if revived == false {
		t.Fatal("a live lease was declared dead")
	}
	if ch == nil {
		t.Fatal("revival returned no keepalive channel")
	}

	if lease := victim.leaseID(); lease != leaseBefore {
		t.Errorf("lease changed from %d to %d on a revived session", leaseBefore, lease)
	}

	createAfter, leaseAfter, found := appKeyInfo(t, victim, appKey)
	if found == false {
		t.Fatal("application route disappeared across the revival")
	}
	if createAfter != createBefore {
		t.Errorf("application key was re-created (create revision %d -> %d) on a revived session",
			createBefore, createAfter)
	}
	if leaseAfter != int64(leaseBefore) {
		t.Errorf("application key moved to lease %d, want the original %d", leaseAfter, leaseBefore)
	}

	// The whole point: the rest of the cluster saw nothing.
	observerNode.ShouldSendEvent().
		Where(eventType[EventNodeLeft]()).
		Since(mark).
		None().
		Assert()
	observerNode.ShouldSendEvent().
		Where(eventType[EventApplicationStopped]()).
		Since(mark).
		None().
		Assert()

	if present, suspect := routeState(observer, "victim-app", victimNode.Name()); present == false || suspect {
		t.Errorf("observer state after the revival: present=%v suspect=%v, want present and healthy", present, suspect)
	}
}

// TestIntegrationReviveLeaseRejectsDeadLease: a lease that is genuinely gone
// must not be mistaken for a live one.
func TestIntegrationReviveLeaseRejectsDeadLease(t *testing.T) {
	endpoints := getTestEndpoints()
	if endpoints == nil {
		t.Skip("Skipping integration test - no ETCD_ENDPOINTS set")
	}

	c, _ := registerNode(t, Options{
		Endpoints:      endpoints,
		Cluster:        "session-dead-lease-cluster",
		RequestTimeout: 5 * time.Second,
		LeaseTTL:       5,
	}, "dead-lease@h")

	ctx, cancel := c.requestContext()
	granted, err := c.cli.Grant(ctx, 5)
	cancel()
	if err != nil {
		t.Fatalf("failed to grant a lease: %v", err)
	}
	c.revokeLease(granted.ID)

	started := time.Now()
	revived, ch := c.reviveLease(c.ctx, granted.ID)
	if revived {
		t.Fatal("a revoked lease was reported alive")
	}
	if ch != nil {
		t.Fatal("revival returned a channel for a dead lease")
	}
	// It must decide from the server's answer, not by waiting out the budget.
	if elapsed := time.Since(started); elapsed > 3*time.Second {
		t.Errorf("deciding a dead lease took %v, the server answers immediately", elapsed)
	}
}

// TestIntegrationWatchSurvivesConnectionDrop pins the end to end property the
// seed plus resume design is there for: a dropped connection loses no event.
func TestIntegrationWatchSurvivesConnectionDrop(t *testing.T) {
	endpoints := getTestEndpoints()
	if endpoints == nil {
		t.Skip("Skipping integration test - no ETCD_ENDPOINTS set")
	}

	const cluster = "watch-recover-cluster"

	proxy, err := NewTestProxy("localhost:6068", endpoints[0])
	if err != nil {
		t.Fatalf("failed to create proxy: %v", err)
	}
	t.Cleanup(func() { proxy.Close() }) // runs after every node has terminated

	watcher, _ := registerNode(t, Options{
		Endpoints: []string{proxy.Addr()},
		Cluster:   cluster,
		LeaseTTL:  30,
	}, "watcher@h")

	publisher, publisherNode := registerNode(t, Options{
		Endpoints: endpoints,
		Cluster:   cluster,
	}, "publisher@h")

	t.Cleanup(func() { proxy.Unblock() })

	if err := publisher.RegisterApplicationRoute(appRoute(publisherNode.Name(), "before-app", 10)); err != nil {
		t.Fatalf("failed to publish: %v", err)
	}
	waitFor(t, 10*time.Second, func() bool {
		present, _ := routeState(watcher, "before-app", publisherNode.Name())
		return present
	}, "watcher never saw the first route")

	leaseBefore := watcher.leaseID()
	rebuiltBefore := watcher.statSessionRebuilt.Load()

	proxy.DropAll()

	// Published while the watcher's stream is down. Resuming at the stored
	// revision is what makes this arrive at all.
	if err := publisher.RegisterApplicationRoute(appRoute(publisherNode.Name(), "during-app", 10)); err != nil {
		t.Fatalf("failed to publish during the outage: %v", err)
	}

	waitFor(t, 60*time.Second, func() bool {
		present, _ := routeState(watcher, "during-app", publisherNode.Name())
		return present
	}, "watcher never received the route published while its watch was down")

	if lease := watcher.leaseID(); lease != leaseBefore {
		t.Errorf("lease changed from %d to %d, a broken watch must not rebuild the session",
			leaseBefore, lease)
	}
	if rebuilt := watcher.statSessionRebuilt.Load(); rebuilt > rebuiltBefore {
		t.Errorf("session rebuilt %d time(s) for a watch failure", rebuilt-rebuiltBefore)
	}

	routes, err := watcher.ResolveApplication("during-app")
	if err != nil {
		t.Fatalf("resolve of the route published during the outage: %v", err)
	}
	if len(routes) != 1 || routes[0].Node != publisherNode.Name() {
		t.Fatalf("resolve returned %v, want the route of %s", routes, publisherNode.Name())
	}
}

// TestIntegrationWatchResyncsAfterCompaction: a resume revision compacted away
// costs a resync of the mirror and nothing else.
func TestIntegrationWatchResyncsAfterCompaction(t *testing.T) {
	endpoints := getTestEndpoints()
	if endpoints == nil {
		t.Skip("Skipping integration test - no ETCD_ENDPOINTS set")
	}

	const cluster = "watch-compaction-cluster"

	proxy, err := NewTestProxy("localhost:6069", endpoints[0])
	if err != nil {
		t.Fatalf("failed to create proxy: %v", err)
	}
	t.Cleanup(func() { proxy.Close() }) // runs after every node has terminated

	// A long lease: the outage below must break the watch, not the session.
	watcher, _ := registerNode(t, Options{
		Endpoints: []string{proxy.Addr()},
		Cluster:   cluster,
		LeaseTTL:  60,
	}, "watcher@h")

	publisher, publisherNode := registerNode(t, Options{
		Endpoints: endpoints,
		Cluster:   cluster,
	}, "publisher@h")

	t.Cleanup(func() { proxy.Unblock() })

	if err := publisher.RegisterApplicationRoute(appRoute(publisherNode.Name(), "before-app", 10)); err != nil {
		t.Fatalf("failed to publish: %v", err)
	}
	waitFor(t, 10*time.Second, func() bool {
		present, _ := routeState(watcher, "before-app", publisherNode.Name())
		return present
	}, "watcher never saw the first route")

	leaseBefore := watcher.leaseID()
	rebuiltBefore := watcher.statSessionRebuilt.Load()
	resyncBefore := watcher.statWatchResync.Load()

	proxy.Block()
	proxy.DropAll()

	// Move the store forward, then compact away everything the watcher would
	// have needed to replay.
	if err := publisher.RegisterApplicationRoute(appRoute(publisherNode.Name(), "during-app", 10)); err != nil {
		t.Fatalf("failed to publish during the outage: %v", err)
	}
	for i := 0; i < 5; i++ {
		if err := publisher.RegisterApplicationRoute(appRoute(publisherNode.Name(), "during-app", 10+i)); err != nil {
			t.Fatalf("failed to bump the revision: %v", err)
		}
	}

	ctx, cancel := publisher.requestContext()
	current, err := publisher.cli.Get(ctx, publisher.pathNodes)
	cancel()
	if err != nil {
		t.Fatalf("failed to read the current revision: %v", err)
	}

	ctx, cancel = publisher.requestContext()
	_, err = publisher.cli.Compact(ctx, current.Header.Revision)
	cancel()
	if err != nil {
		t.Fatalf("failed to compact: %v", err)
	}

	proxy.Unblock()

	waitFor(t, 60*time.Second, func() bool {
		return watcher.statWatchResync.Load() > resyncBefore
	}, "watcher never resynced after its resume revision was compacted away")

	waitFor(t, 30*time.Second, func() bool {
		present, _ := routeState(watcher, "during-app", publisherNode.Name())
		return present
	}, "watcher never picked up the route published during the outage")

	if lease := watcher.leaseID(); lease != leaseBefore {
		t.Errorf("lease changed from %d to %d, a compacted watch must not rebuild the session",
			leaseBefore, lease)
	}
	if rebuilt := watcher.statSessionRebuilt.Load(); rebuilt > rebuiltBefore {
		t.Errorf("session rebuilt %d time(s) for a compaction", rebuilt-rebuiltBefore)
	}
}

// TestIntegrationRegistrarSessionEvents covers the two events phase 4 added.
func TestIntegrationRegistrarSessionEvents(t *testing.T) {
	endpoints := getTestEndpoints()
	if endpoints == nil {
		t.Skip("Skipping integration test - no ETCD_ENDPOINTS set")
	}

	proxy, err := NewTestProxy("localhost:6072", endpoints[0])
	if err != nil {
		t.Fatalf("failed to create proxy: %v", err)
	}
	t.Cleanup(func() { proxy.Close() })

	_, node := registerNode(t, Options{
		Endpoints: []string{proxy.Addr()},
		Cluster:   "session-events-cluster",
		LeaseTTL:  2,
	}, "flapper@h")

	t.Cleanup(func() { proxy.Unblock() })

	node.ShouldSendEvent().
		Where(eventType[EventRegistrarConnected]()).
		Within(10 * time.Second).
		AtLeast(1).
		Assert()

	mark := node.Mark()

	proxy.Block()
	proxy.DropAll()

	node.ShouldSendEvent().
		Where(eventMessage(EventRegistrarDisconnected{Reason: errLeaseLost})).
		Since(mark).
		Within(30 * time.Second).
		AtLeast(1).
		Assert()

	proxy.Unblock()

	node.ShouldSendEvent().
		Where(eventType[EventRegistrarConnected]()).
		Since(mark).
		Within(60 * time.Second).
		AtLeast(1).
		Assert()
}
