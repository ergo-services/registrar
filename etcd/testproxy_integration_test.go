package etcd

import (
	"context"
	"testing"
	"time"

	"ergo.services/ergo/gen"
)

// TestProxyBasicOperation tests that proxy correctly forwards traffic
func TestProxyBasicOperation(t *testing.T) {
	endpoints := getTestEndpoints()
	if endpoints == nil {
		t.Skip("Skipping integration test - no ETCD_ENDPOINTS set")
	}

	// Start proxy between test and etcd
	proxy, err := NewTestProxy("localhost:5050", endpoints[0])
	if err != nil {
		t.Fatalf("Failed to start proxy: %v", err)
	}
	defer proxy.Close()

	// Connect registrar through proxy
	registrar, err := Create(Options{
		Endpoints: []string{"localhost:5050"}, // connect to proxy, not etcd directly
		Cluster:   "test-proxy-basic",
		LeaseTTL:  10,
	})
	if err != nil {
		t.Fatalf("Failed to create registrar: %v", err)
	}
	defer registrar.Terminate()

	client := registrar.(*client)
	node := newMockNode("proxy-test-node")

	routes := gen.RegisterRoutes{
		Routes: []gen.Route{{Host: "localhost", Port: 9001, TLS: false}},
	}

	// Register through proxy
	_, err = client.Register(node, routes)
	if err != nil {
		t.Fatalf("Failed to register through proxy: %v", err)
	}

	// Verify connection works
	stats := proxy.Stats()
	if stats.Accepted == 0 {
		t.Error("Expected proxy to accept connections")
	}

	if stats.Active == 0 {
		t.Error("Expected active connections through proxy")
	}

	t.Logf("Proxy stats: accepted=%d, active=%d", stats.Accepted, stats.Active)
}

// TestProxyNetworkPartition simulates complete network partition
func TestProxyNetworkPartition(t *testing.T) {
	endpoints := getTestEndpoints()
	if endpoints == nil {
		t.Skip("Skipping integration test - no ETCD_ENDPOINTS set")
	}

	proxy, err := NewTestProxy("localhost:5051", endpoints[0])
	if err != nil {
		t.Fatalf("Failed to start proxy: %v", err)
	}
	defer proxy.Close()

	registrar, err := Create(Options{
		Endpoints: []string{"localhost:5051"},
		Cluster:   "test-proxy-partition",
		LeaseTTL:  2, // short TTL for fast test
	})
	if err != nil {
		t.Fatalf("Failed to create registrar: %v", err)
	}
	defer registrar.Terminate()

	client := registrar.(*client)
	node := newMockNode("partition-node")

	routes := gen.RegisterRoutes{
		Routes: []gen.Route{{Host: "localhost", Port: 9001, TLS: false}},
	}

	// Register normally
	_, err = client.Register(node, routes)
	if err != nil {
		t.Fatalf("Failed to register: %v", err)
	}

	initialLease := client.lease
	t.Logf("Initial lease: %d", initialLease)

	// SIMULATE NETWORK PARTITION
	// Step 1: Block new connections
	proxy.Block()
	t.Log("Blocked new connections")

	// Step 2: Drop all existing connections
	proxy.DropAll()
	t.Log("Dropped all connections")

	// Now the node is completely cut off from etcd
	// Its keepAlive will fail, lease will expire
	// But it can't reconnect because proxy is blocked

	// Wait for lease to expire on etcd side
	time.Sleep(3 * time.Second)

	stats := proxy.Stats()
	t.Logf("Partition stats: dropped=%d, blocked=%d", stats.Dropped, stats.Blocked)

	// RESTORE NETWORK
	proxy.Unblock()
	t.Log("Unblocked - network restored")

	// Wait for node to detect network restoration and re-register
	time.Sleep(3 * time.Second)

	newLease := client.lease
	t.Logf("New lease after recovery: %d", newLease)

	if newLease == 0 {
		t.Error("Expected node to re-register after network recovery")
	}

	if newLease == initialLease {
		t.Error("Expected new lease after partition recovery")
	}

	// Verify node is registered in etcd
	key := client.pathNodes + string(node.Name())
	getResp, err := client.cli.Get(context.Background(), key)
	if err != nil {
		t.Fatalf("Failed to get key: %v", err)
	}

	if getResp.Count == 0 {
		t.Fatal("Expected node to be registered after recovery")
	}

	t.Logf("Node successfully recovered after partition")
}

// TestProxyRaceCondition tests race between two nodes during partition recovery
func TestProxyRaceCondition(t *testing.T) {
	endpoints := getTestEndpoints()
	if endpoints == nil {
		t.Skip("Skipping integration test - no ETCD_ENDPOINTS set")
	}

	// Create two proxies for two nodes
	proxy1, err := NewTestProxy("localhost:5052", endpoints[0])
	if err != nil {
		t.Fatalf("Failed to start proxy1: %v", err)
	}
	defer proxy1.Close()

	proxy2, err := NewTestProxy("localhost:5053", endpoints[0])
	if err != nil {
		t.Fatalf("Failed to start proxy2: %v", err)
	}
	defer proxy2.Close()

	// Node1 registers first
	registrar1, err := Create(Options{
		Endpoints: []string{"localhost:5052"},
		Cluster:   "test-proxy-race",
		LeaseTTL:  2,
	})
	if err != nil {
		t.Fatalf("Failed to create registrar1: %v", err)
	}
	defer registrar1.Terminate()

	client1 := registrar1.(*client)
	node1 := newMockNode("race-node")

	routes := gen.RegisterRoutes{
		Routes: []gen.Route{{Host: "localhost", Port: 9001, TLS: false}},
	}

	_, err = client1.Register(node1, routes)
	if err != nil {
		t.Fatalf("Node1 failed to register: %v", err)
	}

	t.Log("Node1 registered")

	// Node1 experiences network partition
	proxy1.Block()
	proxy1.DropAll()
	t.Log("Node1 partitioned")

	// Wait for Node1's lease to expire
	time.Sleep(3 * time.Second)

	// Node2 registers with same name (Node1's lease expired)
	registrar2, err := Create(Options{
		Endpoints: []string{"localhost:5053"},
		Cluster:   "test-proxy-race",
		LeaseTTL:  2,
	})
	if err != nil {
		t.Fatalf("Failed to create registrar2: %v", err)
	}
	defer registrar2.Terminate()

	client2 := registrar2.(*client)
	node2 := newMockNode("race-node") // Same name!

	_, err = client2.Register(node2, routes)
	if err != nil {
		t.Fatalf("Node2 failed to register: %v", err)
	}

	t.Log("Node2 registered with same name")

	// Restore Node1's network
	proxy1.Unblock()
	t.Log("Node1 network restored")

	// Wait for Node1 to attempt re-registration
	time.Sleep(3 * time.Second)

	// Verify Node2 still owns the name (ModRevision protection)
	key := client1.pathNodes + string(node1.Name())
	getResp, err := client1.cli.Get(context.Background(), key)
	if err != nil {
		t.Fatalf("Failed to get key: %v", err)
	}

	if getResp.Count == 0 {
		t.Fatal("Expected key to exist")
	}

	// Key should be owned by Node2's lease
	if getResp.Kvs[0].Lease != int64(client2.lease) {
		t.Errorf("Expected Node2 to own the name (lease %d), but got lease %d",
			client2.lease, getResp.Kvs[0].Lease)
	}

	t.Log("Node2 correctly retained ownership despite Node1 recovery")
}

// TestProxyIntermittentConnection tests flaky network with repeated disconnects
func TestProxyIntermittentConnection(t *testing.T) {
	endpoints := getTestEndpoints()
	if endpoints == nil {
		t.Skip("Skipping integration test - no ETCD_ENDPOINTS set")
	}

	proxy, err := NewTestProxy("localhost:5054", endpoints[0])
	if err != nil {
		t.Fatalf("Failed to start proxy: %v", err)
	}
	defer proxy.Close()

	registrar, err := Create(Options{
		Endpoints: []string{"localhost:5054"},
		Cluster:   "test-proxy-flaky",
		LeaseTTL:  10, // longer TTL to survive disconnects
	})
	if err != nil {
		t.Fatalf("Failed to create registrar: %v", err)
	}
	defer registrar.Terminate()

	client := registrar.(*client)
	node := newMockNode("flaky-node")

	routes := gen.RegisterRoutes{
		Routes: []gen.Route{{Host: "localhost", Port: 9001, TLS: false}},
	}

	_, err = client.Register(node, routes)
	if err != nil {
		t.Fatalf("Failed to register: %v", err)
	}

	initialLease := client.lease

	// Simulate flaky network: drop connections repeatedly
	for i := 0; i < 3; i++ {
		t.Logf("Flake %d: dropping connections", i+1)
		proxy.DropAll()
		time.Sleep(1 * time.Second) // short pause - lease should survive
	}

	// Lease should be the same (keepAlive reconnected each time)
	currentLease := client.lease
	if currentLease != initialLease {
		t.Errorf("Lease changed from %d to %d (should have survived flaky network)",
			initialLease, currentLease)
	}

	// Verify node still registered
	key := client.pathNodes + string(node.Name())
	getResp, err := client.cli.Get(context.Background(), key)
	if err != nil {
		t.Fatalf("Failed to get key: %v", err)
	}

	if getResp.Count == 0 {
		t.Fatal("Expected node to still be registered")
	}

	t.Log("Node survived intermittent network issues")
}

// TestProxyStatistics verifies proxy correctly tracks statistics
func TestProxyStatistics(t *testing.T) {
	endpoints := getTestEndpoints()
	if endpoints == nil {
		t.Skip("Skipping integration test - no ETCD_ENDPOINTS set")
	}

	proxy, err := NewTestProxy("localhost:5055", endpoints[0])
	if err != nil {
		t.Fatalf("Failed to start proxy: %v", err)
	}
	defer proxy.Close()

	// Create registrar
	registrar, err := Create(Options{
		Endpoints: []string{"localhost:5055"},
		Cluster:   "test-proxy-stats",
		LeaseTTL:  10,
	})
	if err != nil {
		t.Fatalf("Failed to create registrar: %v", err)
	}
	defer registrar.Terminate()

	client := registrar.(*client)
	node := newMockNode("stats-node")

	routes := gen.RegisterRoutes{
		Routes: []gen.Route{{Host: "localhost", Port: 9001, TLS: false}},
	}

	// Register
	_, err = client.Register(node, routes)
	if err != nil {
		t.Fatalf("Failed to register: %v", err)
	}

	time.Sleep(500 * time.Millisecond)

	stats1 := proxy.Stats()
	t.Logf("After registration: %+v", stats1)

	if stats1.Accepted == 0 {
		t.Error("Expected accepted connections")
	}

	// Block proxy
	proxy.Block()

	// Try to create another registrar (should fail to connect)
	_, err = Create(Options{
		Endpoints: []string{"localhost:5055"},
		Cluster:   "test-proxy-stats",
		LeaseTTL:  10,
	})
	// This will likely timeout or fail

	time.Sleep(1 * time.Second)

	stats2 := proxy.Stats()
	t.Logf("After blocking: %+v", stats2)

	// Unblock and drop connections
	proxy.Unblock()
	initialActive := proxy.ActiveConnections()
	proxy.DropAll()

	stats3 := proxy.Stats()
	t.Logf("After drop: %+v", stats3)

	if stats3.Dropped < int64(initialActive) {
		t.Errorf("Expected at least %d dropped connections, got %d",
			initialActive, stats3.Dropped)
	}

	finalActive := proxy.ActiveConnections()
	if finalActive != 0 {
		t.Errorf("Expected 0 active connections after drop, got %d", finalActive)
	}
}
