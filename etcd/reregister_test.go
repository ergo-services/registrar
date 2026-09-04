package etcd

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"ergo.services/ergo/gen"
	etcdcli "go.etcd.io/etcd/client/v3"
)

// appKeyInfo returns the create revision and the attached lease of an
// application route key, plus whether the key exists at all.
func appKeyInfo(t *testing.T, c *client, key string) (createRevision int64, lease int64, found bool) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := c.cli.Get(ctx, key)
	if err != nil {
		t.Fatalf("failed to get %s: %v", key, err)
	}
	if resp.Count == 0 {
		return 0, 0, false
	}
	return resp.Kvs[0].CreateRevision, resp.Kvs[0].Lease, true
}

func appRoute(node gen.Atom, name gen.Atom, weight int) gen.ApplicationRoute {
	return gen.ApplicationRoute{
		Node:   node,
		Name:   name,
		Weight: weight,
		State:  gen.ApplicationStateRunning,
		Mode:   gen.ApplicationModeTransient,
	}
}

// TestIntegrationReRegisterKeepsApplicationRoutes: re-registration must
// re-attach application routes to the new lease, not drop and recreate them.
func TestIntegrationReRegisterKeepsApplicationRoutes(t *testing.T) {
	endpoints := getTestEndpoints()
	if endpoints == nil {
		t.Skip("Skipping integration test - no ETCD_ENDPOINTS set")
	}

	options := Options{Endpoints: endpoints, Cluster: "reregister-test-cluster"}
	c, node := registerNode(t, options, "reregister-node")
	eventName := gen.Atom(c.pathClusterRoutes)

	route := appRoute(node.Name(), "keepme", 10)
	if err := c.RegisterApplicationRoute(route); err != nil {
		t.Fatalf("failed to register application route: %v", err)
	}

	appKey := c.pathApps + string(route.Name) + "/" + string(node.Name())
	createBefore, leaseBefore, found := appKeyInfo(t, c, appKey)
	if found == false {
		t.Fatal("application route was not published")
	}

	oldLease := c.leaseID()
	if leaseBefore != int64(oldLease) {
		t.Fatalf("route lease %d, want the node lease %d", leaseBefore, oldLease)
	}

	time.Sleep(300 * time.Millisecond) // let the watch come up
	mark := node.Mark()                // scope assertions to the re-registration

	if err := c.tryReRegister(oldLease); err != nil {
		t.Fatalf("re-registration failed: %v", err)
	}

	newLease := c.leaseID()
	if newLease == oldLease {
		t.Fatal("expected re-registration to switch to a new lease")
	}

	createAfter, leaseAfter, found := appKeyInfo(t, c, appKey)
	if found == false {
		t.Fatal("application route disappeared after re-registration")
	}
	if createAfter != createBefore {
		t.Errorf("application key was deleted and re-created (create revision %d -> %d): "+
			"re-registration must re-attach it to the new lease, not drop it",
			createBefore, createAfter)
	}
	if leaseAfter != int64(newLease) {
		t.Errorf("application key lease %d, want the new lease %d", leaseAfter, newLease)
	}

	// The re-attach is visible as a Put, which also proves the watch delivered
	// post-re-registration events, making the negative assertion below sound.
	node.ShouldSendEvent().
		Name(eventName).
		Where(eventType[EventApplicationStarted]()).
		Since(mark).
		Within(5 * time.Second).
		AtLeast(1).
		Assert()

	node.ShouldSendEvent().
		Where(eventType[EventApplicationStopped]()).
		Since(mark).
		None().
		Assert()
}

// TestIntegrationRequestTimeoutWhenEtcdUnreachable checks that no registrar
// call blocks indefinitely when etcd stops answering.
func TestIntegrationRequestTimeoutWhenEtcdUnreachable(t *testing.T) {
	endpoints := getTestEndpoints()
	if endpoints == nil {
		t.Skip("Skipping integration test - no ETCD_ENDPOINTS set")
	}

	proxy, err := NewTestProxy("localhost:6064", endpoints[0])
	if err != nil {
		t.Fatalf("failed to create proxy: %v", err)
	}
	defer proxy.Close()

	requestTimeout := time.Second
	options := Options{
		Endpoints:      []string{proxy.Addr()},
		Cluster:        "timeout-test-cluster",
		RequestTimeout: requestTimeout,
		LeaseTTL:       2,
	}
	c, node := registerNode(t, options, "timeout-node")

	proxy.Block()
	proxy.DropAll()

	calls := []struct {
		name string
		call func() error
	}{
		{"Nodes", func() error {
			_, err := c.Nodes()
			return err
		}},
		{"RegisterApplicationRoute", func() error {
			return c.RegisterApplicationRoute(appRoute(node.Name(), "blocked-app", 1))
		}},
		{"UnregisterApplicationRoute", func() error {
			return c.UnregisterApplicationRoute("blocked-app")
		}},
		{"Resolve", func() error {
			_, err := c.Resolve("some-node")
			return err
		}},
		{"ResolveApplication", func() error {
			_, err := c.ResolveApplication("never-resolved-app")
			return err
		}},
	}

	for _, tc := range calls {
		t.Run(tc.name, func(t *testing.T) {
			done := make(chan error, 1)
			started := time.Now()
			go func() { done <- tc.call() }()

			select {
			case err := <-done:
				if err == nil {
					t.Fatalf("%s returned no error while etcd is unreachable", tc.name)
				}
				// A few RPCs may be attempted in sequence, so allow slack over
				// the single-call timeout, but nothing near "forever".
				if elapsed := time.Since(started); elapsed > 10*requestTimeout {
					t.Errorf("%s took %v, want roughly %v", tc.name, elapsed, requestTimeout)
				}
			case <-time.After(30 * time.Second):
				t.Fatalf("%s did not return: the call has no deadline", tc.name)
			}
		})
	}
}

// TestIntegrationRouteRegistrationDuringLeaseSwitch hammers
// RegisterApplicationRoute from several goroutines while the lease is
// replaced underneath them.
func TestIntegrationRouteRegistrationDuringLeaseSwitch(t *testing.T) {
	endpoints := getTestEndpoints()
	if endpoints == nil {
		t.Skip("Skipping integration test - no ETCD_ENDPOINTS set")
	}

	options := Options{Endpoints: endpoints, Cluster: "lease-switch-test-cluster"}
	c, node := registerNode(t, options, "lease-switch-node")

	const writers = 8
	stop := make(chan struct{})
	var wg sync.WaitGroup

	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			route := appRoute(node.Name(), gen.Atom(fmt.Sprintf("switch-app-%d", i)), i+1)
			for {
				if err := c.RegisterApplicationRoute(route); err != nil {
					t.Errorf("registering %s failed: %v", route.Name, err)
					return
				}
				select {
				case <-stop:
					return
				default:
				}
			}
		}(i)
	}

	for i := 0; i < 3; i++ {
		if err := c.tryReRegister(c.leaseID()); err != nil {
			t.Errorf("re-registration %d failed: %v", i, err)
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	close(stop)
	wg.Wait()

	// Revoking the leases that keepRegistration was keeping alive makes it
	// re-register on its own, so wait until the lease stops changing before
	// asserting which lease the keys are attached to.
	lease := waitLeaseStable(t, c, 15*time.Second)

	for i := 0; i < writers; i++ {
		name := fmt.Sprintf("switch-app-%d", i)
		key := c.pathApps + name + "/" + string(node.Name())
		_, keyLease, found := appKeyInfo(t, c, key)
		if found == false {
			t.Errorf("route %s was lost across the lease switches", name)
			continue
		}
		if keyLease != int64(lease) {
			t.Errorf("route %s attached to lease %d, want the current lease %d",
				name, keyLease, lease)
		}
	}
}

// waitLeaseStable waits until the lease stays unchanged for a full second and
// returns it.
func waitLeaseStable(t *testing.T, c *client, timeout time.Duration) etcdcli.LeaseID {
	t.Helper()

	deadline := time.Now().Add(timeout)
	last := c.leaseID()
	stableSince := time.Now()

	for time.Now().Before(deadline) {
		time.Sleep(100 * time.Millisecond)
		current := c.leaseID()
		if current != last {
			last = current
			stableSince = time.Now()
			continue
		}
		if time.Since(stableSince) >= time.Second {
			return current
		}
	}

	t.Fatalf("lease kept changing for %v", timeout)
	return 0
}
