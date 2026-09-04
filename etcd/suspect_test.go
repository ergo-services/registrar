package etcd

import (
	"testing"
	"time"

	"ergo.services/ergo/gen"
)

// waitFor polls cond until it holds or the timeout expires.
func waitFor(t *testing.T, timeout time.Duration, cond func() bool, format string, args ...any) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf(format, args...)
}

// routeState reports what an observer currently believes about one route.
func routeState(c *client, app, node gen.Atom) (present bool, suspect bool) {
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
	return true, suspect
}

// TestIntegrationSuspectRouteSurvivesLeaseExpiry is the CORE-701 scenario: a
// healthy node loses its lease, and resolve must keep answering.
func TestIntegrationSuspectRouteSurvivesLeaseExpiry(t *testing.T) {
	endpoints := getTestEndpoints()
	if endpoints == nil {
		t.Skip("Skipping integration test - no ETCD_ENDPOINTS set")
	}

	const cluster = "suspect-survive-cluster"

	proxy, err := NewTestProxy("localhost:6065", endpoints[0])
	if err != nil {
		t.Fatalf("failed to create proxy: %v", err)
	}
	t.Cleanup(func() { proxy.Close() }) // runs after every node has terminated

	// The observer keeps a healthy session throughout, so its suspicion clock
	// runs. A short grace keeps the test quick.
	observer, _ := registerNode(t, Options{
		Endpoints:     endpoints,
		Cluster:       cluster,
		SuspectGrace:  4 * time.Second,
		SweepInterval: 500 * time.Millisecond,
	}, "observer@h")

	victim, victimNode := registerNode(t, Options{
		Endpoints: []string{proxy.Addr()},
		Cluster:   cluster,
		LeaseTTL:  2,
	}, "victim@h")

	t.Cleanup(func() { proxy.Unblock() }) // let the victim revoke its lease on the way out

	route := appRoute(victimNode.Name(), "victim-app", 10)
	if err := victim.RegisterApplicationRoute(route); err != nil {
		t.Fatalf("failed to publish the application route: %v", err)
	}

	waitFor(t, 10*time.Second, func() bool {
		present, suspect := routeState(observer, "victim-app", victimNode.Name())
		return present && suspect == false
	}, "observer never saw the route as healthy")

	// Cut the victim off. Its lease expires and etcd removes everything it owns.
	proxy.Block()
	proxy.DropAll()

	waitFor(t, 15*time.Second, func() bool {
		_, suspect := routeState(observer, "victim-app", victimNode.Name())
		return suspect
	}, "observer never marked the route suspect after the lease expired")

	// This is the assertion the whole design exists for.
	routes, err := observer.ResolveApplication("victim-app")
	if err != nil {
		t.Fatalf("resolve failed while the route was merely suspect: %v", err)
	}
	if len(routes) != 1 || routes[0].Node != victimNode.Name() {
		t.Fatalf("resolve returned %v, want the suspect route of %s", routes, victimNode.Name())
	}

	// And it must not last forever: the grace ends and the route goes.
	waitFor(t, 15*time.Second, func() bool {
		_, err := observer.ResolveApplication("victim-app")
		return err == gen.ErrNoRoute
	}, "suspect route outlived its grace")
}

// TestIntegrationSuspectClearedOnReturn: the session comes back inside the
// grace, so nobody downstream ever loses the route.
func TestIntegrationSuspectClearedOnReturn(t *testing.T) {
	endpoints := getTestEndpoints()
	if endpoints == nil {
		t.Skip("Skipping integration test - no ETCD_ENDPOINTS set")
	}

	const cluster = "suspect-return-cluster"

	proxy, err := NewTestProxy("localhost:6066", endpoints[0])
	if err != nil {
		t.Fatalf("failed to create proxy: %v", err)
	}
	t.Cleanup(func() { proxy.Close() }) // runs after every node has terminated

	// Default grace, far longer than the outage below.
	observer, _ := registerNode(t, Options{
		Endpoints: endpoints,
		Cluster:   cluster,
	}, "observer@h")

	victim, victimNode := registerNode(t, Options{
		Endpoints: []string{proxy.Addr()},
		Cluster:   cluster,
		LeaseTTL:  2,
	}, "victim@h")

	t.Cleanup(func() { proxy.Unblock() })

	if err := victim.RegisterApplicationRoute(appRoute(victimNode.Name(), "victim-app", 10)); err != nil {
		t.Fatalf("failed to publish the application route: %v", err)
	}

	waitFor(t, 10*time.Second, func() bool {
		present, suspect := routeState(observer, "victim-app", victimNode.Name())
		return present && suspect == false
	}, "observer never saw the route as healthy")

	stop := make(chan struct{})
	failures := make(chan error, 1)
	go func() {
		for {
			select {
			case <-stop:
				close(failures)
				return
			default:
			}
			if _, err := observer.ResolveApplication("victim-app"); err != nil {
				select {
				case failures <- err:
				default:
				}
				close(failures)
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
	}()

	proxy.Block()
	proxy.DropAll()

	waitFor(t, 15*time.Second, func() bool {
		_, suspect := routeState(observer, "victim-app", victimNode.Name())
		return suspect
	}, "observer never marked the route suspect after the lease expired")

	proxy.Unblock()

	waitFor(t, 30*time.Second, func() bool {
		present, suspect := routeState(observer, "victim-app", victimNode.Name())
		return present && suspect == false
	}, "route never recovered after the victim re-registered")

	close(stop)
	if err, ok := <-failures; ok && err != nil {
		t.Fatalf("resolve failed during the outage: %v", err)
	}
}

// TestIntegrationGracefulShutdownIsImmediate: a deliberate shutdown must not
// be mistaken for the failure the grace exists for.
func TestIntegrationGracefulShutdownIsImmediate(t *testing.T) {
	endpoints := getTestEndpoints()
	if endpoints == nil {
		t.Skip("Skipping integration test - no ETCD_ENDPOINTS set")
	}

	const cluster = "graceful-shutdown-cluster"

	// A long grace: if the departure is read as an inference, the route lingers
	// far beyond the deadline below and this test says so.
	observer, observerNode := registerNode(t, Options{
		Endpoints:    endpoints,
		Cluster:      cluster,
		SuspectGrace: 60 * time.Second,
	}, "observer@h")

	leaver, leaverNode := registerNode(t, Options{
		Endpoints: endpoints,
		Cluster:   cluster,
	}, "leaver@h")

	if err := leaver.RegisterApplicationRoute(appRoute(leaverNode.Name(), "leaver-app", 10)); err != nil {
		t.Fatalf("failed to publish the application route: %v", err)
	}

	waitFor(t, 10*time.Second, func() bool {
		present, suspect := routeState(observer, "leaver-app", leaverNode.Name())
		return present && suspect == false
	}, "observer never saw the route as healthy")

	mark := observerNode.Mark()
	leaver.Terminate()

	waitFor(t, 5*time.Second, func() bool {
		_, err := observer.ResolveApplication("leaver-app")
		return err == gen.ErrNoRoute
	}, "route of a node that shut down cleanly is still being resolved")

	waitFor(t, 5*time.Second, func() bool {
		present, _ := routeState(observer, "leaver-app", leaverNode.Name())
		return present == false
	}, "route of a node that shut down cleanly is still in the mirror")

	observerNode.ShouldSendEvent().
		Where(eventMessage(EventNodeLeft{Name: leaverNode.Name()})).
		Since(mark).
		Within(5 * time.Second).
		AtLeast(1).
		Assert()
}

// TestIntegrationInferredLossIsAnnouncedOnlyAfterGrace: events carry belief,
// not observation, so an unattributed loss is announced only after the grace.
func TestIntegrationInferredLossIsAnnouncedOnlyAfterGrace(t *testing.T) {
	endpoints := getTestEndpoints()
	if endpoints == nil {
		t.Skip("Skipping integration test - no ETCD_ENDPOINTS set")
	}

	const cluster = "event-gate-cluster"

	proxy, err := NewTestProxy("localhost:6070", endpoints[0])
	if err != nil {
		t.Fatalf("failed to create proxy: %v", err)
	}
	t.Cleanup(func() { proxy.Close() })

	observer, observerNode := registerNode(t, Options{
		Endpoints:     endpoints,
		Cluster:       cluster,
		SuspectGrace:  5 * time.Second,
		SweepInterval: 500 * time.Millisecond,
	}, "observer@h")

	victim, victimNode := registerNode(t, Options{
		Endpoints: []string{proxy.Addr()},
		Cluster:   cluster,
		LeaseTTL:  2,
	}, "victim@h")

	t.Cleanup(func() { proxy.Unblock() })

	if err := victim.RegisterApplicationRoute(appRoute(victimNode.Name(), "victim-app", 10)); err != nil {
		t.Fatalf("failed to publish the application route: %v", err)
	}
	waitFor(t, 10*time.Second, func() bool {
		present, suspect := routeState(observer, "victim-app", victimNode.Name())
		return present && suspect == false
	}, "observer never saw the route as healthy")

	mark := observerNode.Mark()

	proxy.Block()
	proxy.DropAll()

	waitFor(t, 15*time.Second, func() bool {
		_, suspect := routeState(observer, "victim-app", victimNode.Name())
		return suspect
	}, "observer never marked the route suspect")

	// The keys are gone from etcd and the observer knows it, but it has not
	// concluded anything yet, so the cluster has not been told.
	observerNode.ShouldSendEvent().
		Where(eventType[EventApplicationStopped]()).
		Since(mark).
		None().
		Assert()
	observerNode.ShouldSendEvent().
		Where(eventType[EventNodeLeft]()).
		Since(mark).
		None().
		Assert()

	// Once the grace runs out the loss is believed, and only then announced.
	observerNode.ShouldSendEvent().
		Where(eventMessage(EventApplicationStopped{Name: "victim-app", Node: victimNode.Name()})).
		Since(mark).
		Within(15 * time.Second).
		AtLeast(1).
		Assert()
	observerNode.ShouldSendEvent().
		Where(eventMessage(EventNodeLeft{Name: victimNode.Name()})).
		Since(mark).
		Within(15 * time.Second).
		AtLeast(1).
		Assert()
}

// TestIntegrationRecoveredLossIsNeverAnnounced: a loss cancelled by the node
// coming back inside the grace is never announced at all.
func TestIntegrationRecoveredLossIsNeverAnnounced(t *testing.T) {
	endpoints := getTestEndpoints()
	if endpoints == nil {
		t.Skip("Skipping integration test - no ETCD_ENDPOINTS set")
	}

	const cluster = "event-gate-recover-cluster"

	proxy, err := NewTestProxy("localhost:6071", endpoints[0])
	if err != nil {
		t.Fatalf("failed to create proxy: %v", err)
	}
	t.Cleanup(func() { proxy.Close() })

	observer, observerNode := registerNode(t, Options{
		Endpoints: endpoints,
		Cluster:   cluster,
	}, "observer@h")

	victim, victimNode := registerNode(t, Options{
		Endpoints: []string{proxy.Addr()},
		Cluster:   cluster,
		LeaseTTL:  2,
	}, "victim@h")

	t.Cleanup(func() { proxy.Unblock() })

	if err := victim.RegisterApplicationRoute(appRoute(victimNode.Name(), "victim-app", 10)); err != nil {
		t.Fatalf("failed to publish the application route: %v", err)
	}
	waitFor(t, 10*time.Second, func() bool {
		present, suspect := routeState(observer, "victim-app", victimNode.Name())
		return present && suspect == false
	}, "observer never saw the route as healthy")

	mark := observerNode.Mark()

	proxy.Block()
	proxy.DropAll()
	waitFor(t, 15*time.Second, func() bool {
		_, suspect := routeState(observer, "victim-app", victimNode.Name())
		return suspect
	}, "observer never marked the route suspect")
	proxy.Unblock()

	waitFor(t, 30*time.Second, func() bool {
		present, suspect := routeState(observer, "victim-app", victimNode.Name())
		return present && suspect == false
	}, "route never recovered")

	observerNode.ShouldSendEvent().
		Where(eventType[EventApplicationStopped]()).
		Since(mark).
		None().
		Assert()
	observerNode.ShouldSendEvent().
		Where(eventType[EventNodeLeft]()).
		Since(mark).
		None().
		Assert()
}
