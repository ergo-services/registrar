package etcd

import (
	"fmt"
	"testing"

	"ergo.services/ergo/gen"
)

func TestEncodeDecodeRoute(t *testing.T) {
	original := []gen.Route{
		{
			Host: "localhost",
			Port: 8080,
			TLS:  false,
		},
		{
			Host: "example.com",
			Port: 9090,
			TLS:  true,
		},
	}

	// Test encoding
	encoded, err := encode(original)
	if err != nil {
		t.Errorf("encode() error = %v", err)
		return
	}

	if encoded == "" {
		t.Error("encode() returned empty string")
		return
	}

	// Test decoding
	decoded, err := decode([]byte(encoded))
	if err != nil {
		t.Errorf("decode() error = %v", err)
		return
	}

	routes, ok := decoded.([]gen.Route)
	if !ok {
		t.Errorf("decode() returned wrong type: %T", decoded)
		return
	}

	if len(routes) != len(original) {
		t.Errorf("Expected %d routes, got %d", len(original), len(routes))
		return
	}

	for i, route := range routes {
		if route.Host != original[i].Host {
			t.Errorf("Route[%d].Host = %v, want %v", i, route.Host, original[i].Host)
		}
		if route.Port != original[i].Port {
			t.Errorf("Route[%d].Port = %v, want %v", i, route.Port, original[i].Port)
		}
		if route.TLS != original[i].TLS {
			t.Errorf("Route[%d].TLS = %v, want %v", i, route.TLS, original[i].TLS)
		}
	}
}

func TestEncodeDecodeApplicationRoute(t *testing.T) {
	original := gen.ApplicationRoute{
		Name:   "test-app",
		Node:   "test-node",
		State:  gen.ApplicationStateRunning,
		Weight: 100,
		Mode:   gen.ApplicationModeTransient,
	}

	// Test encoding
	encoded, err := encode(original)
	if err != nil {
		t.Errorf("encode() error = %v", err)
		return
	}

	if encoded == "" {
		t.Error("encode() returned empty string")
		return
	}

	// Test decoding
	decoded, err := decode([]byte(encoded))
	if err != nil {
		t.Errorf("decode() error = %v", err)
		return
	}

	route, ok := decoded.(gen.ApplicationRoute)
	if !ok {
		t.Errorf("decode() returned wrong type: %T", decoded)
		return
	}

	if route.Name != original.Name {
		t.Errorf("ApplicationRoute.Name = %v, want %v", route.Name, original.Name)
	}
	if route.Node != original.Node {
		t.Errorf("ApplicationRoute.Node = %v, want %v", route.Node, original.Node)
	}
	if route.State != original.State {
		t.Errorf("ApplicationRoute.State = %v, want %v", route.State, original.State)
	}
	if route.Weight != original.Weight {
		t.Errorf("ApplicationRoute.Weight = %v, want %v", route.Weight, original.Weight)
	}
	if route.Mode != original.Mode {
		t.Errorf("ApplicationRoute.Mode = %v, want %v", route.Mode, original.Mode)
	}
}

func TestEncodeDecodeSimpleTypes(t *testing.T) {
	tests := []struct {
		name     string
		original any
	}{
		{"string", "test-string"},
		{"int", 42},
		{"bool", true},
		{"float", 3.14},
		{"slice", []string{"a", "b", "c"}},
		{"map", map[string]int{"one": 1, "two": 2}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test encoding
			encoded, err := encode(tt.original)
			if err != nil {
				t.Errorf("encode() error = %v", err)
				return
			}

			if encoded == "" {
				t.Error("encode() returned empty string")
				return
			}

			// Test decoding
			decoded, err := decode([]byte(encoded))
			if err != nil {
				t.Errorf("decode() error = %v", err)
				return
			}

			// Note: Due to EDF encoding/decoding, exact type might not be preserved
			// but the value should be equivalent
			if decoded == nil {
				t.Error("decode() returned nil")
			}
		})
	}
}

func TestDecodeInvalidData(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		{"empty", []byte{}},
		{"invalid base64", []byte("invalid-base64-data!!!")},
		{"invalid edf", []byte("dGVzdA==")}, // "test" in base64, but not valid EDF
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := decode(tt.data)
			if err == nil {
				t.Error("decode() expected error but got none")
			}
		})
	}
}

// TestDecodeConfigValueTypes tests the new typed configuration decoding
func TestDecodeConfigValueTypes(t *testing.T) {
	testCases := []struct {
		input         string
		expectedType  string
		expectedValue any
		desc          string
	}{
		{"int:123", "int64", int64(123), "positive integer"},
		{"int:-456", "int64", int64(-456), "negative integer"},
		{"int:0", "int64", int64(0), "zero integer"},
		{"float:3.14", "float64", float64(3.14), "positive float"},
		{"float:-2.5", "float64", float64(-2.5), "negative float"},
		{"float:0.0", "float64", float64(0.0), "zero float"},
		{"bool:true", "bool", bool(true), "boolean true"},
		{"bool:false", "bool", bool(false), "boolean false"},
		{"plain string", "string", "plain string", "string without prefix"},
		{"int: not a number", "string", "int: not a number", "malformed int with space stays as string"},
		{"float: not a number", "string", "float: not a number", "malformed float with space stays as string"},
		{"bool: invalid", "string", "bool: invalid", "malformed bool with space stays as string"},
		{"", "string", "", "empty string"},
		{"int:invalid", "string", "int:invalid", "invalid int stays as string"},
		{"float:badnum", "string", "float:badnum", "invalid float stays as string"},
		{"bool:invalid", "string", "bool:invalid", "invalid bool stays as string"},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			result, err := decodeConfigValue(tc.input)

			if err != nil {
				t.Fatalf("Unexpected error for %s: %v", tc.input, err)
			}

			// Check value
			if result != tc.expectedValue {
				t.Errorf("Expected %v, got %v", tc.expectedValue, result)
			}

			// Check type
			switch tc.expectedType {
			case "int64":
				if _, ok := result.(int64); !ok {
					t.Errorf("Expected int64, got %T", result)
				}
			case "float64":
				if _, ok := result.(float64); !ok {
					t.Errorf("Expected float64, got %T", result)
				}
			case "string":
				if _, ok := result.(string); !ok {
					t.Errorf("Expected string, got %T", result)
				}
			case "bool":
				if _, ok := result.(bool); !ok {
					t.Errorf("Expected bool, got %T", result)
				}
			}
		})
	}
}

// TestDecodeConfigValueNonString tests that non-string inputs are passed through unchanged
func TestDecodeConfigValueNonString(t *testing.T) {
	testCases := []any{
		123,
		3.14,
		true,
	}

	for _, input := range testCases {
		t.Run(fmt.Sprintf("%T", input), func(t *testing.T) {
			result, err := decodeConfigValue(input)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if result != input {
				t.Errorf("Expected %v, got %v", input, result)
			}
		})
	}
}
