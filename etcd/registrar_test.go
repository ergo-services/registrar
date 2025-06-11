package etcd

import (
	"os"
	"strings"
	"testing"
	"time"

	"ergo.services/ergo/gen"
)

// getTestEndpoints returns etcd endpoints for testing
// If ETCD_ENDPOINTS environment variable is set (Docker environment), use those
// Otherwise, skip tests that require actual etcd connection
func getTestEndpoints() []string {
	if endpoints := os.Getenv("ETCD_ENDPOINTS"); endpoints != "" {
		return strings.Split(endpoints, ",")
	}
	return nil
}

func TestCreate(t *testing.T) {
	tests := []struct {
		name               string
		options            Options
		wantErr            bool
		requiresConnection bool
	}{
		{
			name:               "default options",
			options:            Options{},
			wantErr:            false,
			requiresConnection: true, // This test creates a real etcd client connection
		},
		{
			name: "custom cluster",
			options: Options{
				Cluster: "test-cluster",
			},
			wantErr:            false,
			requiresConnection: true,
		},
		{
			name: "with authentication",
			options: Options{
				Username: "testuser",
				Password: "testpass",
			},
			wantErr:            false, // Client creation succeeds even if auth not enabled on server
			requiresConnection: true,
		},
		{
			name: "with custom timeouts",
			options: Options{
				DialTimeout:    10 * time.Second,
				RequestTimeout: 5 * time.Second,
				KeepAlive:      15 * time.Second,
			},
			wantErr:            false,
			requiresConnection: true,
		},
	}

	etcdEndpoints := getTestEndpoints()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Skip tests that require etcd connection if no endpoints available
			if tt.requiresConnection && etcdEndpoints == nil {
				t.Skip("Skipping test that requires etcd connection - no ETCD_ENDPOINTS set")
				return
			}

			// Use test endpoints only if test requires connection AND endpoints are available
			if etcdEndpoints != nil && tt.requiresConnection {
				tt.options.Endpoints = etcdEndpoints
			}

			registrar, err := Create(tt.options)
			if (err != nil) != tt.wantErr {
				t.Errorf("Create() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if err == nil {
				client := registrar.(*client)

				// Check default values are set
				if client.options.Cluster == "" {
					t.Error("Expected default cluster to be set")
				}
				if len(client.options.Endpoints) == 0 {
					t.Error("Expected default endpoints to be set")
				}
				if client.options.DialTimeout == 0 {
					t.Error("Expected default dial timeout to be set")
				}
				if client.options.RequestTimeout == 0 {
					t.Error("Expected default request timeout to be set")
				}
				if client.options.KeepAlive == 0 {
					t.Error("Expected default keep alive to be set")
				}

				// Check paths are constructed correctly
				expectedCluster := tt.options.Cluster
				if expectedCluster == "" {
					expectedCluster = "default"
				}
				expectedPath := "services/ergo/cluster/" + expectedCluster + "/"
				if client.pathCluster != expectedPath {
					t.Errorf("Expected pathCluster = %s, got %s", expectedPath, client.pathCluster)
				}

				// Clean up the client
				client.Terminate()
			}
		})
	}
}

func TestDefaultValues(t *testing.T) {
	// Test that constants are set correctly
	if defaultDialTimeout != 10*time.Second {
		t.Errorf("Expected defaultDialTimeout = 10s, got %v", defaultDialTimeout)
	}
	if defaultRequestTimeout != 10*time.Second {
		t.Errorf("Expected defaultRequestTimeout = 10s, got %v", defaultRequestTimeout)
	}
	if defaultKeepAlive != 10*time.Second {
		t.Errorf("Expected defaultKeepAlive = 10s, got %v", defaultKeepAlive)
	}
}

func TestClientInfo(t *testing.T) {
	client := &client{}
	info := client.Info()

	if info.EmbeddedServer {
		t.Error("Expected EmbeddedServer to be false")
	}
	if !info.SupportConfig {
		t.Error("Expected SupportConfig to be true")
	}
	if !info.SupportEvent {
		t.Error("Expected SupportEvent to be true")
	}
	if info.SupportRegisterProxy {
		t.Error("Expected SupportRegisterProxy to be false")
	}
	if !info.SupportRegisterApplication {
		t.Error("Expected SupportRegisterApplication to be true")
	}
}

func TestVersion(t *testing.T) {
	client := &client{}
	version := client.Version()

	if version.Name != clientName {
		t.Errorf("Expected Name = %s, got %s", clientName, version.Name)
	}
	if version.Release != clientRelease {
		t.Errorf("Expected Release = %s, got %s", clientRelease, version.Release)
	}
	if version.License != gen.LicenseMIT {
		t.Errorf("Expected License = %s, got %s", gen.LicenseMIT, version.License)
	}
}
