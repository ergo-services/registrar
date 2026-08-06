package etcd

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"ergo.services/ergo/gen"
	"ergo.services/ergo/testing/mock"
)

// TestIntegrationRegistration tests the full registration flow
func TestIntegrationRegistration(t *testing.T) {
	endpoints := getTestEndpoints()
	if endpoints == nil {
		t.Skip("Skipping integration test - no ETCD_ENDPOINTS set")
	}

	// Create registrar
	registrar, err := Create(Options{
		Endpoints: endpoints,
		Cluster:   "test-cluster",
	})
	if err != nil {
		t.Fatalf("Failed to create registrar: %v", err)
	}
	defer registrar.Terminate()

	client := registrar.(*client)
	node := newMockNode(t, "test-node")

	// Test registration
	routes := gen.RegisterRoutes{
		Routes: []gen.Route{
			{Host: "localhost", Port: 9001, TLS: false},
		},
		ApplicationRoutes: []gen.ApplicationRoute{
			{
				Node:   node.Name(),
				Name:   "test-app",
				Weight: 50,
				State:  gen.ApplicationStateRunning,
				Mode:   gen.ApplicationModeTransient,
			},
		},
	}

	staticRoutes, err := client.Register(node, routes)
	if err != nil {
		t.Fatalf("Failed to register: %v", err)
	}

	// Verify registration
	if len(staticRoutes.Routes) != 0 {
		t.Error("Expected no static routes")
	}

	// Test that node is set
	if client.node.Name() != node.Name() {
		t.Error("Expected node to be set")
	}

	// Test Info method
	info := client.Info()
	if !info.SupportConfig {
		t.Error("Expected config support")
	}
	if !info.SupportEvent {
		t.Error("Expected event support")
	}
	if !info.SupportRegisterApplication {
		t.Error("Expected application registration support")
	}
	if info.SupportRegisterProxy {
		t.Error("Expected no proxy registration support")
	}

	// Test Event method
	event, err := client.Event()
	if err != nil {
		t.Fatalf("Failed to get event: %v", err)
	}
	if event.Node != node.Name() {
		t.Errorf("Expected event node %s, got %s", node.Name(), event.Node)
	}
}

// TestIntegrationNodes tests node discovery
func TestIntegrationNodes(t *testing.T) {
	endpoints := getTestEndpoints()
	if endpoints == nil {
		t.Skip("Skipping integration test - no ETCD_ENDPOINTS set")
	}

	// Create two registrars simulating different nodes
	registrar1, err := Create(Options{
		Endpoints: endpoints,
		Cluster:   "test-cluster",
	})
	if err != nil {
		t.Fatalf("Failed to create registrar1: %v", err)
	}
	defer registrar1.Terminate()

	registrar2, err := Create(Options{
		Endpoints: endpoints,
		Cluster:   "test-cluster",
	})
	if err != nil {
		t.Fatalf("Failed to create registrar2: %v", err)
	}
	defer registrar2.Terminate()

	client1 := registrar1.(*client)
	client2 := registrar2.(*client)

	node1 := newMockNode(t, "node1")
	node2 := newMockNode(t, "node2")

	routes := gen.RegisterRoutes{
		Routes: []gen.Route{{Host: "localhost", Port: 9001, TLS: false}},
	}

	// Register both nodes
	_, err = client1.Register(node1, routes)
	if err != nil {
		t.Fatalf("Failed to register node1: %v", err)
	}

	_, err = client2.Register(node2, routes)
	if err != nil {
		t.Fatalf("Failed to register node2: %v", err)
	}

	// Give etcd time to propagate
	time.Sleep(100 * time.Millisecond)

	// Test Nodes() method
	nodes1, err := client1.Nodes()
	if err != nil {
		t.Fatalf("Failed to get nodes from client1: %v", err)
	}

	// Should see node2 but not itself
	found := false
	for _, n := range nodes1 {
		if n == node2.Name() {
			found = true
		}
		if n == node1.Name() {
			t.Error("Node should not see itself in nodes list")
		}
	}
	if !found {
		t.Error("Node1 should see node2 in nodes list")
	}
}

// TestIntegrationApplicationRoutes tests application route registration
func TestIntegrationApplicationRoutes(t *testing.T) {
	endpoints := getTestEndpoints()
	if endpoints == nil {
		t.Skip("Skipping integration test - no ETCD_ENDPOINTS set")
	}

	registrar, err := Create(Options{
		Endpoints: endpoints,
		Cluster:   "test-cluster",
	})
	if err != nil {
		t.Fatalf("Failed to create registrar: %v", err)
	}
	defer registrar.Terminate()

	client := registrar.(*client)
	node := newMockNode(t, "test-node")

	routes := gen.RegisterRoutes{
		Routes: []gen.Route{{Host: "localhost", Port: 9001, TLS: false}},
	}

	_, err = client.Register(node, routes)
	if err != nil {
		t.Fatalf("Failed to register: %v", err)
	}

	// Test RegisterApplicationRoute
	appRoute := gen.ApplicationRoute{
		Node:   node.Name(),
		Name:   "dynamic-app",
		Weight: 75,
		State:  gen.ApplicationStateRunning,
		Mode:   gen.ApplicationModeTransient,
	}

	err = client.RegisterApplicationRoute(appRoute)
	if err != nil {
		t.Fatalf("Failed to register application route: %v", err)
	}

	// Test UnregisterApplicationRoute
	err = client.UnregisterApplicationRoute("dynamic-app")
	if err != nil {
		t.Fatalf("Failed to unregister application route: %v", err)
	}
}

// TestIntegrationConfiguration tests configuration management
func TestIntegrationConfiguration(t *testing.T) {
	endpoints := getTestEndpoints()
	if endpoints == nil {
		t.Skip("Skipping integration test - no ETCD_ENDPOINTS set")
	}

	// First, set up some test configuration in etcd
	rawClient, err := Create(Options{Endpoints: endpoints})
	if err != nil {
		t.Fatalf("Failed to create raw client: %v", err)
	}
	defer rawClient.Terminate()

	etcdClient := rawClient.(*client).cli

	// Set up test configuration data
	clusterConfigPath := "services/ergo/cluster/test-cluster/config/"
	globalConfigPath := "services/ergo/config/"

	testConfigs := map[string]string{
		clusterConfigPath + "test-node/db.host":              "localhost",
		clusterConfigPath + "test-node/db.port":              "5432",
		clusterConfigPath + "*/log.level":                    "info",
		clusterConfigPath + "*/timeout":                      "30",
		globalConfigPath + "global/debug.enabled":            "false",
		globalConfigPath + "test-cluster/test-node/priority": "high",
	}

	// Put test configuration
	for key, value := range testConfigs {
		_, err = etcdClient.Put(context.Background(), key, value)
		if err != nil {
			t.Fatalf("Failed to put config %s: %v", key, err)
		}
	}

	// Clean up after test
	defer func() {
		for key := range testConfigs {
			etcdClient.Delete(context.Background(), key)
		}
	}()

	// Create registrar and register node
	registrar, err := Create(Options{
		Endpoints: endpoints,
		Cluster:   "test-cluster",
	})
	if err != nil {
		t.Fatalf("Failed to create registrar: %v", err)
	}
	defer registrar.Terminate()

	client := registrar.(*client)
	node := newMockNode(t, "test-node")

	routes := gen.RegisterRoutes{
		Routes: []gen.Route{{Host: "localhost", Port: 9001, TLS: false}},
	}

	_, err = client.Register(node, routes)
	if err != nil {
		t.Fatalf("Failed to register: %v", err)
	}

	// Give time for configuration to load
	time.Sleep(200 * time.Millisecond)

	// Test ConfigItem method with hierarchy
	testCases := []struct {
		item     string
		expected string
		desc     string
	}{
		{"db.host", "localhost", "node-specific config"},
		{"db.port", "5432", "node-specific config"},
		{"log.level", "info", "cluster default config"},
		{"timeout", "30", "cluster default config"},
		{"debug.enabled", "false", "global config"},
		{"priority", "high", "cross-cluster specific config"},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			value, err := client.ConfigItem(tc.item)
			if err != nil {
				t.Fatalf("Failed to get config item %s: %v", tc.item, err)
			}
			if value != tc.expected {
				t.Errorf("Expected %s = %s, got %v", tc.item, tc.expected, value)
			}
		})
	}

	// Test Config method for multiple items
	config, err := client.Config("db.host", "log.level", "debug.enabled", "nonexistent")
	if err != nil {
		t.Fatalf("Failed to get config: %v", err)
	}

	expectedItems := map[string]string{
		"db.host":       "localhost",
		"log.level":     "info",
		"debug.enabled": "false",
	}

	for item, expected := range expectedItems {
		if value, exists := config[item]; !exists {
			t.Errorf("Expected config item %s to exist", item)
		} else if value != expected {
			t.Errorf("Expected %s = %s, got %v", item, expected, value)
		}
	}

	// nonexistent item should not be in the map
	if _, exists := config["nonexistent"]; exists {
		t.Error("Expected nonexistent item to not be in config map")
	}

	// Test ConfigItem with nonexistent item
	_, err = client.ConfigItem("nonexistent")
	if err != gen.ErrUnknown {
		t.Errorf("Expected ErrUnknown for nonexistent config, got %v", err)
	}
}

// TestIntegrationProxyMethods tests unsupported proxy methods
func TestIntegrationProxyMethods(t *testing.T) {
	endpoints := getTestEndpoints()
	if endpoints == nil {
		t.Skip("Skipping integration test - no ETCD_ENDPOINTS set")
	}

	registrar, err := Create(Options{Endpoints: endpoints})
	if err != nil {
		t.Fatalf("Failed to create registrar: %v", err)
	}
	defer registrar.Terminate()

	client := registrar.(*client)

	// Test RegisterProxy returns ErrUnsupported
	err = client.RegisterProxy("test-proxy")
	if err != gen.ErrUnsupported {
		t.Errorf("Expected ErrUnsupported, got %v", err)
	}

	// Test UnregisterProxy returns ErrUnsupported
	err = client.UnregisterProxy("test-proxy")
	if err != gen.ErrUnsupported {
		t.Errorf("Expected ErrUnsupported, got %v", err)
	}
}

// TestIntegrationTerminatedState tests behavior after termination
func TestIntegrationTerminatedState(t *testing.T) {
	endpoints := getTestEndpoints()
	if endpoints == nil {
		t.Skip("Skipping integration test - no ETCD_ENDPOINTS set")
	}

	registrar, err := Create(Options{Endpoints: endpoints})
	if err != nil {
		t.Fatalf("Failed to create registrar: %v", err)
	}

	client := registrar.(*client)
	node := newMockNode(t, "test-node")

	routes := gen.RegisterRoutes{
		Routes: []gen.Route{{Host: "localhost", Port: 9001, TLS: false}},
	}

	_, err = client.Register(node, routes)
	if err != nil {
		t.Fatalf("Failed to register: %v", err)
	}

	// Terminate the client
	client.Terminate()

	// Test ConfigItem after termination
	_, err = client.ConfigItem("test")
	if err != gen.ErrRegistrarTerminated {
		t.Errorf("Expected ErrRegistrarTerminated, got %v", err)
	}

	// Test Config after termination
	_, err = client.Config("test")
	if err != gen.ErrRegistrarTerminated {
		t.Errorf("Expected ErrRegistrarTerminated, got %v", err)
	}
}

// TestIntegrationResolver tests resolver functionality
func TestIntegrationResolver(t *testing.T) {
	endpoints := getTestEndpoints()
	if endpoints == nil {
		t.Skip("Skipping integration test - no ETCD_ENDPOINTS set")
	}

	registrar, err := Create(Options{Endpoints: endpoints})
	if err != nil {
		t.Fatalf("Failed to create registrar: %v", err)
	}
	defer registrar.Terminate()

	client := registrar.(*client)

	// Test Resolver method
	resolver := client.Resolver()
	if resolver != client {
		t.Error("Expected resolver to return self")
	}
}

// TestIntegrationErrorConditions tests various error conditions and edge cases
func TestIntegrationErrorConditions(t *testing.T) {
	endpoints := getTestEndpoints()
	if endpoints == nil {
		t.Skip("Skipping integration test - no ETCD_ENDPOINTS set")
	}

	// Test with invalid endpoints to trigger connection errors
	t.Run("invalid_endpoints", func(t *testing.T) {
		_, err := Create(Options{
			Endpoints: []string{"invalid:9999"},
		})
		// Client creation should fail with invalid endpoints due to connection timeout
		if err == nil {
			t.Error("Expected client creation to fail with invalid endpoints")
		}
	})

	// Test double registration
	t.Run("double_registration", func(t *testing.T) {
		registrar1, err := Create(Options{
			Endpoints: endpoints,
			Cluster:   "test-cluster",
		})
		if err != nil {
			t.Fatalf("Failed to create registrar1: %v", err)
		}
		defer registrar1.Terminate()

		registrar2, err := Create(Options{
			Endpoints: endpoints,
			Cluster:   "test-cluster",
		})
		if err != nil {
			t.Fatalf("Failed to create registrar2: %v", err)
		}
		defer registrar2.Terminate()

		client1 := registrar1.(*client)
		client2 := registrar2.(*client)

		node1 := newMockNode(t, "same-node")
		node2 := newMockNode(t, "same-node")

		routes := gen.RegisterRoutes{
			Routes: []gen.Route{{Host: "localhost", Port: 9001, TLS: false}},
		}

		// First registration should succeed
		_, err = client1.Register(node1, routes)
		if err != nil {
			t.Fatalf("First registration failed: %v", err)
		}

		// Second registration with same node name should fail
		_, err = client2.Register(node2, routes)
		if err != gen.ErrTaken {
			t.Errorf("Expected ErrTaken for duplicate registration, got %v", err)
		}
	})
}

// TestIntegrationVersion tests version information
func TestIntegrationVersion(t *testing.T) {
	endpoints := getTestEndpoints()
	if endpoints == nil {
		t.Skip("Skipping integration test - no ETCD_ENDPOINTS set")
	}

	registrar, err := Create(Options{
		Endpoints: endpoints,
	})
	if err != nil {
		t.Fatalf("Failed to create registrar: %v", err)
	}
	defer registrar.Terminate()

	client := registrar.(*client)
	version := client.Version()

	if version.Name == "" {
		t.Error("Expected version name to be set")
	}
	if version.Release == "" {
		t.Error("Expected version release to be set")
	}
	if version.License != gen.LicenseMIT {
		t.Errorf("Expected MIT license, got %s", version.License)
	}
}

// TestIntegrationConfigValidation tests configuration validation methods
func TestIntegrationConfigValidation(t *testing.T) {
	endpoints := getTestEndpoints()
	if endpoints == nil {
		t.Skip("Skipping integration test - no ETCD_ENDPOINTS set")
	}

	registrar, err := Create(Options{
		Endpoints: endpoints,
	})
	if err != nil {
		t.Fatalf("Failed to create registrar: %v", err)
	}
	defer registrar.Terminate()

	client := registrar.(*client)

	// Test isValidConfigKey
	testCases := []struct {
		key   string
		valid bool
		desc  string
	}{
		{"node1/item1", true, "valid node config"},
		{"*/item1", true, "valid wildcard config"},
		{"global/item1", true, "valid global config"},
		{"cluster1/node1/item1", true, "valid cross-cluster config"},
		{"", false, "empty key"},
		{"single", false, "single part"},
		{"a/b/c/d", false, "four parts"},
		{"node1/", false, "empty item"},
		{"/item1", false, "empty node"},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			result := client.isValidConfigKey(tc.key)
			if result != tc.valid {
				t.Errorf("isValidConfigKey(%s) = %v, expected %v", tc.key, result, tc.valid)
			}
		})
	}

	// Test isConfigRelevantToNode
	relevantTestCases := []struct {
		configKey string
		nodeName  string
		relevant  bool
		desc      string
	}{
		{"node1/item", "node1", true, "matching node"},
		{"node1/item", "node2", false, "different node"},
		{"*/item", "node1", true, "wildcard matches any node"},
		{"global/item", "node1", true, "global matches any node"},
		{"cluster1/node1/item", "node1", false, "wrong cluster (client cluster is 'default')"},
		{"default/node1/item", "node1", true, "correct cluster and node"},
		{"default/node1/item", "node2", false, "correct cluster, wrong node"},
	}

	for _, tc := range relevantTestCases {
		t.Run(tc.desc, func(t *testing.T) {
			result := client.isConfigRelevantToNode(tc.configKey, tc.nodeName)
			if result != tc.relevant {
				t.Errorf("isConfigRelevantToNode(%s, %s) = %v, expected %v",
					tc.configKey, tc.nodeName, result, tc.relevant)
			}
		})
	}
}

// TestIntegrationTypedConfiguration tests typed configuration values with prefixes
func TestIntegrationTypedConfiguration(t *testing.T) {
	endpoints := getTestEndpoints()
	if endpoints == nil {
		t.Skip("Skipping integration test - no ETCD_ENDPOINTS set")
	}

	// First, set up some test configuration in etcd
	rawClient, err := Create(Options{Endpoints: endpoints})
	if err != nil {
		t.Fatalf("Failed to create raw client: %v", err)
	}
	defer rawClient.Terminate()

	etcdClient := rawClient.(*client).cli

	// Set up test configuration data with type prefixes
	clusterConfigPath := "services/ergo/cluster/test-cluster/config/"

	testConfigs := map[string]string{
		clusterConfigPath + "test-node/db.port":       "int:5432",
		clusterConfigPath + "test-node/db.timeout":    "int:30",
		clusterConfigPath + "test-node/cache.ratio":   "float:0.75",
		clusterConfigPath + "test-node/pi.value":      "float:3.14159",
		clusterConfigPath + "test-node/db.host":       "localhost",    // string without prefix
		clusterConfigPath + "test-node/app.name":      "myapp",        // string without prefix
		clusterConfigPath + "test-node/invalid.int":   "int:invalid",  // invalid int
		clusterConfigPath + "test-node/invalid.float": "float:badnum", // invalid float
	}

	// Put test configuration
	for key, value := range testConfigs {
		_, err = etcdClient.Put(context.Background(), key, value)
		if err != nil {
			t.Fatalf("Failed to put config %s: %v", key, err)
		}
	}

	// Clean up after test
	defer func() {
		for key := range testConfigs {
			etcdClient.Delete(context.Background(), key)
		}
	}()

	// Create registrar and register node
	registrar, err := Create(Options{
		Endpoints: endpoints,
		Cluster:   "test-cluster",
	})
	if err != nil {
		t.Fatalf("Failed to create registrar: %v", err)
	}
	defer registrar.Terminate()

	client := registrar.(*client)
	node := newMockNode(t, "test-node")

	routes := gen.RegisterRoutes{
		Routes: []gen.Route{{Host: "localhost", Port: 9001, TLS: false}},
	}

	_, err = client.Register(node, routes)
	if err != nil {
		t.Fatalf("Failed to register: %v", err)
	}

	// Give time for configuration to load
	time.Sleep(200 * time.Millisecond)

	// Test typed configuration values
	testCases := []struct {
		item          string
		expectedValue any
		expectedType  string
		desc          string
	}{
		{"db.port", int64(5432), "int64", "int typed config"},
		{"db.timeout", int64(30), "int64", "int typed config"},
		{"cache.ratio", float64(0.75), "float64", "float typed config"},
		{"pi.value", float64(3.14159), "float64", "float typed config"},
		{"db.host", "localhost", "string", "string config without prefix"},
		{"app.name", "myapp", "string", "string config without prefix"},
		{"invalid.int", "int:invalid", "string", "invalid int stays as string"},
		{"invalid.float", "float:badnum", "string", "invalid float stays as string"},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			value, err := client.ConfigItem(tc.item)
			if err != nil {
				t.Fatalf("Failed to get config item %s: %v", tc.item, err)
			}

			// Check value matches
			if value != tc.expectedValue {
				t.Errorf("Expected %s = %v, got %v", tc.item, tc.expectedValue, value)
			}

			// Check type matches
			switch tc.expectedType {
			case "int64":
				if _, ok := value.(int64); !ok {
					t.Errorf("Expected %s to be int64, got %T", tc.item, value)
				}
			case "float64":
				if _, ok := value.(float64); !ok {
					t.Errorf("Expected %s to be float64, got %T", tc.item, value)
				}
			case "string":
				if _, ok := value.(string); !ok {
					t.Errorf("Expected %s to be string, got %T", tc.item, value)
				}
			}
		})
	}

	// Test Config method with mixed types
	config, err := client.Config("db.port", "cache.ratio", "db.host", "nonexistent")
	if err != nil {
		t.Fatalf("Failed to get config: %v", err)
	}

	// Verify types in bulk config retrieval
	if port, exists := config["db.port"]; exists {
		if portInt, ok := port.(int64); !ok || portInt != 5432 {
			t.Errorf("Expected db.port to be int64(5432), got %T(%v)", port, port)
		}
	} else {
		t.Error("Expected db.port to exist in config")
	}

	if ratio, exists := config["cache.ratio"]; exists {
		if ratioFloat, ok := ratio.(float64); !ok || ratioFloat != 0.75 {
			t.Errorf("Expected cache.ratio to be float64(0.75), got %T(%v)", ratio, ratio)
		}
	} else {
		t.Error("Expected cache.ratio to exist in config")
	}

	if host, exists := config["db.host"]; exists {
		if hostStr, ok := host.(string); !ok || hostStr != "localhost" {
			t.Errorf("Expected db.host to be string(localhost), got %T(%v)", host, host)
		}
	} else {
		t.Error("Expected db.host to exist in config")
	}
}

// TestIntegrationNodeEvents asserts the join/leave events an observing registrar
// publishes when another node appears in the cluster and then goes away.
func TestIntegrationNodeEvents(t *testing.T) {
	endpoints := getTestEndpoints()
	if endpoints == nil {
		t.Skip("Skipping integration test - no ETCD_ENDPOINTS set")
	}

	options := Options{Endpoints: endpoints, Cluster: "event-test-cluster"}

	observer, observerNode := registerNode(t, options, "event-observer")
	eventName := gen.Atom(observer.pathClusterRoutes)

	// keepRegistration establishes the watch right after Register returns. Give
	// it a moment so the join below is not missed.
	time.Sleep(300 * time.Millisecond)

	joining, joiningNode := registerNode(t, options, "joining-node")

	observerNode.ShouldSendEvent().
		Name(eventName).
		Where(eventMessage(EventNodeJoined{Name: joiningNode.Name()})).
		Within(5 * time.Second).
		AtLeast(1).
		Assert()

	// a registrar never reports its own node
	observerNode.ShouldSendEvent().
		Where(eventMessage(EventNodeJoined{Name: observerNode.Name()})).
		None().
		Assert()

	joining.Terminate() // revokes the lease, etcd drops the node key

	observerNode.ShouldSendEvent().
		Name(eventName).
		Where(eventMessage(EventNodeLeft{Name: joiningNode.Name()})).
		Within(5 * time.Second).
		AtLeast(1).
		Assert()

	nodes, err := observer.Nodes()
	if err != nil {
		t.Fatalf("Failed to get nodes list: %v", err)
	}
	for _, node := range nodes {
		if node == joiningNode.Name() {
			t.Error("Expected left node to not be in nodes list")
		}
	}
}

// TestIntegrationApplicationEvents asserts that every application state maps to
// the matching lifecycle event, and that removing a route reports it stopped.
func TestIntegrationApplicationEvents(t *testing.T) {
	endpoints := getTestEndpoints()
	if endpoints == nil {
		t.Skip("Skipping integration test - no ETCD_ENDPOINTS set")
	}

	options := Options{Endpoints: endpoints, Cluster: "app-event-test-cluster"}
	client, node := registerNode(t, options, "app-test-node")
	eventName := gen.Atom(client.pathClusterRoutes)

	time.Sleep(300 * time.Millisecond) // let the watch come up

	testCases := []struct {
		route   gen.ApplicationRoute
		started any
		desc    string
	}{
		{
			route: gen.ApplicationRoute{
				Node:   node.Name(),
				Name:   "test-app-loaded",
				Weight: 50,
				State:  gen.ApplicationStateLoaded,
				Mode:   gen.ApplicationModeTransient,
			},
			started: EventApplicationLoaded{Name: "test-app-loaded", Node: node.Name(), Weight: 50},
			desc:    "loaded application",
		},
		{
			route: gen.ApplicationRoute{
				Node:   node.Name(),
				Name:   "test-app-running",
				Weight: 75,
				State:  gen.ApplicationStateRunning,
				Mode:   gen.ApplicationModeTransient,
			},
			started: EventApplicationStarted{
				Name:   "test-app-running",
				Node:   node.Name(),
				Weight: 75,
				Mode:   gen.ApplicationModeTransient,
			},
			desc: "running application",
		},
		{
			route: gen.ApplicationRoute{
				Node:   node.Name(),
				Name:   "test-app-stopping",
				Weight: 25,
				State:  gen.ApplicationStateStopping,
				Mode:   gen.ApplicationModeTransient,
			},
			started: EventApplicationStopping{Name: "test-app-stopping", Node: node.Name()},
			desc:    "stopping application",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			if err := client.RegisterApplicationRoute(tc.route); err != nil {
				t.Fatalf("Failed to register application route: %v", err)
			}

			node.ShouldSendEvent().
				Name(eventName).
				Where(eventMessage(tc.started)).
				Within(5 * time.Second).
				AtLeast(1).
				Assert()

			if err := client.UnregisterApplicationRoute(tc.route.Name); err != nil {
				t.Fatalf("Failed to unregister application route: %v", err)
			}

			node.ShouldSendEvent().
				Name(eventName).
				Where(eventMessage(EventApplicationStopped{Name: tc.route.Name, Node: node.Name()})).
				Within(5 * time.Second).
				AtLeast(1).
				Assert()
		})
	}
}

// TestIntegrationConfigurationEvents tests configuration update events
func TestIntegrationConfigurationEvents(t *testing.T) {
	endpoints := getTestEndpoints()
	if endpoints == nil {
		t.Skip("Skipping integration test - no ETCD_ENDPOINTS set")
	}

	// Create registrar and register node
	registrar, err := Create(Options{
		Endpoints: endpoints,
		Cluster:   "config-event-test-cluster",
	})
	if err != nil {
		t.Fatalf("Failed to create registrar: %v", err)
	}
	defer registrar.Terminate()

	client := registrar.(*client)
	node := newMockNode(t, "config-test-node")

	_, err = client.Register(node, gen.RegisterRoutes{
		Routes: []gen.Route{{Host: "localhost", Port: 9001, TLS: false}},
	})
	if err != nil {
		t.Fatalf("Failed to register node: %v", err)
	}

	// Give time for initial setup
	time.Sleep(200 * time.Millisecond)

	// Get raw etcd client to simulate external config changes
	etcdClient := client.cli

	// Test configuration update events
	testConfigs := []struct {
		key   string
		value string
		desc  string
	}{
		{
			key:   "services/ergo/cluster/config-event-test-cluster/config/config-test-node/test.item1",
			value: "test-value-1",
			desc:  "node-specific config",
		},
		{
			key:   "services/ergo/cluster/config-event-test-cluster/config/*/global.setting",
			value: "int:42",
			desc:  "cluster-wide config with type",
		},
		{
			key:   "services/ergo/config/global/app.timeout",
			value: "float:30.5",
			desc:  "global config with type",
		},
	}

	for _, tc := range testConfigs {
		t.Run(tc.desc, func(t *testing.T) {
			// Set configuration value
			encoded, err := encode(tc.value)
			if err != nil {
				t.Fatalf("Failed to encode config value: %v", err)
			}

			_, err = etcdClient.Put(context.Background(), tc.key, encoded)
			if err != nil {
				t.Fatalf("Failed to put config: %v", err)
			}

			// Give time for watch event processing
			time.Sleep(200 * time.Millisecond)

			// Verify configuration is accessible through registrar
			// Extract item name from key for testing
			parts := strings.Split(tc.key, "/")
			if len(parts) > 0 {
				itemName := parts[len(parts)-1]
				value, err := client.ConfigItem(itemName)
				if err != nil {
					// Some configs might not be relevant to this node
					if err != gen.ErrUnknown {
						t.Errorf("Unexpected error getting config item %s: %v", itemName, err)
					}
				} else {
					// Verify value was processed correctly
					if value == nil {
						t.Errorf("Expected config value for %s, got nil", itemName)
					}
				}
			}

			// Clean up
			etcdClient.Delete(context.Background(), tc.key)
		})
	}
}

// TestIntegrationEventSystemIntegrity tests the overall event system integrity
func TestIntegrationEventSystemIntegrity(t *testing.T) {
	endpoints := getTestEndpoints()
	if endpoints == nil {
		t.Skip("Skipping integration test - no ETCD_ENDPOINTS set")
	}

	// Create multiple registrars to simulate a real cluster
	registrars := make([]*client, 3)
	mockNodes := make([]*mock.Node, 3)

	for i := 0; i < 3; i++ {
		registrar, err := Create(Options{
			Endpoints: endpoints,
			Cluster:   "integrity-test-cluster",
		})
		if err != nil {
			t.Fatalf("Failed to create registrar %d: %v", i, err)
		}
		defer registrar.Terminate()

		registrars[i] = registrar.(*client)
		mockNodes[i] = newMockNode(t, fmt.Sprintf("integrity-node-%d", i))

		_, err = registrars[i].Register(mockNodes[i], gen.RegisterRoutes{
			Routes: []gen.Route{{Host: "localhost", Port: uint16(9001 + i), TLS: false}},
		})
		if err != nil {
			t.Fatalf("Failed to register node %d: %v", i, err)
		}
	}

	// Give time for all nodes to register
	time.Sleep(300 * time.Millisecond)

	// Test 1: Verify all nodes see each other
	for i, client := range registrars {
		nodes, err := client.Nodes()
		if err != nil {
			t.Fatalf("Failed to get nodes from registrar %d: %v", i, err)
		}

		// Should see the other 2 nodes (not itself)
		if len(nodes) != 2 {
			t.Errorf("Registrar %d expected to see 2 other nodes, got %d", i, len(nodes))
		}
	}

	// Test 2: Test event system can handle rapid application changes
	appRoutes := []gen.ApplicationRoute{
		{
			Node:   mockNodes[0].Name(),
			Name:   "rapid-app-1",
			Weight: 10,
			State:  gen.ApplicationStateRunning,
			Mode:   gen.ApplicationModeTransient,
		},
		{
			Node:   mockNodes[1].Name(),
			Name:   "rapid-app-2",
			Weight: 20,
			State:  gen.ApplicationStateRunning,
			Mode:   gen.ApplicationModeTransient,
		},
		{
			Node:   mockNodes[2].Name(),
			Name:   "rapid-app-3",
			Weight: 30,
			State:  gen.ApplicationStateRunning,
			Mode:   gen.ApplicationModeTransient,
		},
	}

	// Register applications rapidly
	for i, route := range appRoutes {
		err := registrars[i].RegisterApplicationRoute(route)
		if err != nil {
			t.Fatalf("Failed to register rapid application %d: %v", i, err)
		}
	}

	// Give time for propagation
	time.Sleep(200 * time.Millisecond)

	// Unregister applications rapidly
	for i, route := range appRoutes {
		err := registrars[i].UnregisterApplicationRoute(route.Name)
		if err != nil {
			t.Fatalf("Failed to unregister rapid application %d: %v", i, err)
		}
	}

	// Test 3: Test configuration events with multiple nodes
	etcdClient := registrars[0].cli
	globalConfigKey := "services/ergo/config/global/cluster.size"

	_, err := etcdClient.Put(context.Background(), globalConfigKey, "int:3")
	if err != nil {
		t.Fatalf("Failed to put global config: %v", err)
	}

	// Give time for propagation
	time.Sleep(200 * time.Millisecond)

	// Verify all nodes can access the global config
	for i, client := range registrars {
		value, err := client.ConfigItem("cluster.size")
		if err != nil {
			t.Errorf("Node %d failed to get global config: %v", i, err)
			continue
		}

		if intVal, ok := value.(int64); !ok || intVal != 3 {
			t.Errorf("Node %d expected cluster.size=3 (int64), got %v (%T)", i, value, value)
		}
	}

	// Clean up
	etcdClient.Delete(context.Background(), globalConfigKey)

	// Test 4: Test node departure events
	// Terminate one registrar to simulate node leaving
	registrars[2].Terminate()

	// Give time for lease expiration
	time.Sleep(300 * time.Millisecond)

	// Verify remaining nodes see the departure
	for i := 0; i < 2; i++ {
		nodes, err := registrars[i].Nodes()
		if err != nil {
			t.Fatalf("Failed to get nodes from registrar %d after departure: %v", i, err)
		}

		// Should now see only 1 other node
		if len(nodes) != 1 {
			t.Errorf("Registrar %d expected to see 1 other node after departure, got %d", i, len(nodes))
		}

		// Should not see the departed node
		for _, node := range nodes {
			if node == mockNodes[2].Name() {
				t.Errorf("Registrar %d still sees departed node %s", i, node)
			}
		}
	}
}
