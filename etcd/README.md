# etcd Registrar

A `gen.Registrar` implementation for [etcd](https://etcd.io/) - a distributed, reliable key-value store for the most critical data of a distributed system.

## Features

- **Distributed Service Discovery**: No single point of failure
- **Hierarchical Configuration**: Four-level priority system with type conversion
- **Real-time Events**: Cluster change notifications via optimized single watcher
- **Automatic Cleanup**: Lease-based node registration with TTL
- **Type Conversion**: `"int:123"` → `int64(123)`, `"float:3.14"` → `float64(3.14)`, `"bool:true"` → `true` from strings
- **Security**: TLS and authentication support
- **High Availability**: Built on etcd's proven consensus algorithm
- **Efficient Watching**: Single watcher with intelligent event routing for minimal resource usage

## Quick Start

```go
import "ergo.services/registrar/etcd"

registrar, err := etcd.Create(etcd.Options{
    Endpoints: []string{"localhost:2379"},
    Cluster:   "production",
    LeaseTTL:  10, // Lease TTL in seconds (default: 10)
})

options.Network.Registrar = registrar
node, err := ergo.StartNode("demo@localhost", options)
```

## Configuration Options

- **Endpoints**: etcd server addresses (default: `["localhost:2379"]`)
- **Cluster**: Cluster name for isolation (default: `"default"`)
- **LeaseTTL**: Lease time-to-live in seconds (default: `10`)
  - Used for node registration keepalive
  - Can be set to 1-2 seconds for faster testing
- **Username/Password**: Authentication credentials
- **TLS**: TLS configuration for secure connections
- **DialTimeout**: Connection timeout (default: 10s)
- **RequestTimeout**: Request timeout (default: 5s)
- **KeepAlive**: Connection keepalive interval (default: 30s)

## Path Structure

### Routes & Service Discovery
- **Nodes**: `services/ergo/cluster/{cluster}/routes/nodes/{node}`
- **Applications**: `services/ergo/cluster/{cluster}/routes/applications/{app}/{node}`

### Configuration Hierarchy (Priority: highest → lowest)
1. Cross-cluster node-specific: `services/ergo/config/{cluster}/{node}/{item}`
2. Cluster node-specific: `services/ergo/cluster/{cluster}/config/{node}/{item}`  
3. Cluster-wide default: `services/ergo/cluster/{cluster}/config/*/{item}`
4. Global default: `services/ergo/config/global/{item}`

### Watch Architecture
Uses a **single efficient watcher** on `services/ergo` prefix with intelligent event routing:
- Routes and applications are encoded with `edf.Encode + base64`
- Configuration values use string encoding with type prefixes
- Events are automatically routed to appropriate handlers based on path

## Type Conversion

Configuration values support automatic type conversion using prefixes:

- **Strings**: `"hello"` → `string("hello")` (no prefix)
- **Integers**: `"int:123"` → `int64(123)`
- **Floats**: `"float:3.14"` → `float64(3.14)`
- **Booleans**: `"bool:true"` → `true`, `"bool:false"` → `false`

Example:
```bash
etcdctl put "services/ergo/cluster/prod/config/cache.enabled" "bool:true"
etcdctl put "services/ergo/cluster/prod/config/cache.size" "int:1024"
etcdctl put "services/ergo/cluster/prod/config/cache.ratio" "float:0.75"
etcdctl put "services/ergo/cluster/prod/config/app.name" "my-service"
```

## Testing

### Running Tests

```bash
# Start etcd via Docker
make start-etcd

# Run all tests with coverage
make test-coverage

# Integration tests only
make test-integration

# Cleanup
make clean
```

### Test Infrastructure

The test suite includes a **TestProxy** - a lightweight TCP proxy for simulating network conditions without external dependencies:

```go
// Create proxy between client and etcd
proxy, _ := NewTestProxy("localhost:5050", "localhost:2379")

// Simulate network partition
proxy.Block()        // Block new connections
proxy.DropAll()      // Drop all active connections

// Restore connectivity
proxy.Unblock()

// Get statistics
stats := proxy.Stats() // accepted, dropped, blocked, active connections
```

**TestProxy enables realistic testing of:**
- Network partitions and recovery
- Lease expiration scenarios
- Race conditions between nodes
- Multiple failure/recovery cycles
- Automatic re-registration logic

### Test Scenarios

The test suite covers:
- **Basic Operations**: Registration, configuration, routes, events
- **Network Resilience**: Automatic re-registration after partition
- **Race Conditions**: Multiple nodes competing for same name
- **Failure Recovery**: Multiple partition/recovery cycles
- **Lease Management**: Expiration and renewal under various conditions
- **Distributed Systems Edge Cases**: Timing windows and consistency

## Test Coverage

- **Comprehensive test suite** with unit and integration tests
- **Unit tests**: Configuration hierarchy, type conversion, validation, encoding/decoding
- **Integration tests**: Real etcd operations with 15+ scenarios
- **Network simulation tests**: TestProxy-based partition/recovery testing
- **Edge case testing**: Race conditions, timing windows, distributed systems consistency
- **Error handling**: Connection failures, invalid data, terminated states

## Documentation

See [documentation](https://docs.ergo.services/extra-library/registrars/etcd-client) for complete usage guide and examples. 
