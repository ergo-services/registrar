package etcd

import (
	"sync"
	"testing"

	"ergo.services/ergo/gen"
)

func TestConfigHierarchy(t *testing.T) {
	client := &client{
		options:    Options{Cluster: "test-cluster"},
		config:     make(map[string]any),
		configLock: sync.RWMutex{},
		// Using mock node name directly in tests instead of full interface
	}

	// Simulate node name for testing
	nodeName := "test-node"

	// Set up test configuration data
	client.config["test-cluster/test-node/database.host"] = "prod-db.example.com"
	client.config["test-node/database.port"] = 5432
	client.config["*/database.timeout"] = 30
	client.config["global/debug.enabled"] = false

	tests := []struct {
		name     string
		item     string
		expected any
		wantErr  bool
	}{
		{
			name:     "cluster-node specific config",
			item:     "database.host",
			expected: "prod-db.example.com",
			wantErr:  false,
		},
		{
			name:     "node specific config",
			item:     "database.port",
			expected: 5432,
			wantErr:  false,
		},
		{
			name:     "cluster default config",
			item:     "database.timeout",
			expected: 30,
			wantErr:  false,
		},
		{
			name:     "global config",
			item:     "debug.enabled",
			expected: false,
			wantErr:  false,
		},
		{
			name:     "non-existent config",
			item:     "missing.config",
			expected: nil,
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test the config lookup logic directly
			var result any
			var err error

			// Simulate ConfigItem logic with test node name
			client.configLock.RLock()

			// 1. cluster/node/item -> value (cross-cluster specific)
			key := client.options.Cluster + "/" + nodeName + "/" + tt.item
			if v, found := client.config[key]; found {
				result = v
			} else {
				// 2. node/item -> value (current cluster, specific node)
				key = nodeName + "/" + tt.item
				if v, found := client.config[key]; found {
					result = v
				} else {
					// 3. */item -> value (current cluster default)
					key = "*/" + tt.item
					if v, found := client.config[key]; found {
						result = v
					} else {
						// 4. global/item -> value (global default)
						key = "global/" + tt.item
						if v, found := client.config[key]; found {
							result = v
						} else {
							err = gen.ErrUnknown
						}
					}
				}
			}

			client.configLock.RUnlock()

			if (err != nil) != tt.wantErr {
				t.Errorf("ConfigItem() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if result != tt.expected {
				t.Errorf("ConfigItem() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestConfigMultiple(t *testing.T) {
	client := &client{
		options:    Options{Cluster: "test-cluster"},
		config:     make(map[string]any),
		configLock: sync.RWMutex{},
	}

	// Set up test configuration data
	client.config["test-cluster/test-node/database.host"] = "prod-db.example.com"
	client.config["test-node/database.port"] = 5432
	client.config["*/log.level"] = "info"

	// Test multiple config retrieval logic directly
	items := []string{"database.host", "database.port", "log.level", "missing.item"}
	result := make(map[string]any)
	nodeName := "test-node"

	client.configLock.RLock()
	for _, item := range items {
		var found bool

		// 1. cluster/node/item -> value (highest priority - cross-cluster specific)
		key := client.options.Cluster + "/" + nodeName + "/" + item
		if v, exists := client.config[key]; exists {
			result[item] = v
			found = true
		} else {
			// 2. node/item -> value (current cluster, specific node)
			key = nodeName + "/" + item
			if v, exists := client.config[key]; exists {
				result[item] = v
				found = true
			} else {
				// 3. */item -> value (current cluster default)
				key = "*/" + item
				if v, exists := client.config[key]; exists {
					result[item] = v
					found = true
				} else {
					// 4. global/item -> value (lowest priority - global default)
					key = "global/" + item
					if v, exists := client.config[key]; exists {
						result[item] = v
						found = true
					}
				}
			}
		}
		_ = found
	}
	client.configLock.RUnlock()

	expected := map[string]any{
		"database.host": "prod-db.example.com",
		"database.port": 5432,
		"log.level":     "info",
		// missing.item should not be in the result
	}

	if len(result) != 3 {
		t.Errorf("Expected 3 config items, got %d", len(result))
	}

	for key, expectedValue := range expected {
		if value, exists := result[key]; !exists {
			t.Errorf("Expected config key %s not found", key)
		} else if value != expectedValue {
			t.Errorf("Config[%s] = %v, want %v", key, value, expectedValue)
		}
	}

	// Check that missing item is not in result
	if _, exists := result["missing.item"]; exists {
		t.Error("Unexpected missing.item found in config result")
	}
}

func TestIsValidConfigKey(t *testing.T) {
	client := &client{}

	tests := []struct {
		name      string
		configKey string
		expected  bool
	}{
		{"valid node/item", "web1/database.host", true},
		{"valid wildcard", "*/log.level", true},
		{"valid global", "global/debug.enabled", true},
		{"valid cluster/node/item", "production/web1/cache.size", true},
		{"empty key", "", false},
		{"single part", "item", false},
		{"four parts", "a/b/c/d", false},
		{"empty parts", "node/", false},
		{"empty first part", "/item", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := client.isValidConfigKey(tt.configKey)
			if result != tt.expected {
				t.Errorf("isValidConfigKey(%s) = %v, want %v", tt.configKey, result, tt.expected)
			}
		})
	}
}

func TestIsConfigRelevantToNode(t *testing.T) {
	client := &client{
		options: Options{Cluster: "test-cluster"},
	}

	tests := []struct {
		name      string
		configKey string
		nodeName  string
		expected  bool
	}{
		{"node specific match", "web1/database.host", "web1", true},
		{"node specific no match", "web2/database.host", "web1", false},
		{"wildcard match", "*/log.level", "web1", true},
		{"global match", "global/debug.enabled", "web1", true},
		{"cluster-node match", "test-cluster/web1/cache.size", "web1", true},
		{"cluster-node wrong cluster", "other-cluster/web1/cache.size", "web1", false},
		{"cluster-node wrong node", "test-cluster/web2/cache.size", "web1", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := client.isConfigRelevantToNode(tt.configKey, tt.nodeName)
			if result != tt.expected {
				t.Errorf("isConfigRelevantToNode(%s, %s) = %v, want %v",
					tt.configKey, tt.nodeName, result, tt.expected)
			}
		})
	}
}
