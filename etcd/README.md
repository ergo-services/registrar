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
})

options.Network.Registrar = registrar
node, err := ergo.StartNode("demo@localhost", options)
```

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

## Coverage

- **74.8%** test coverage
- Unit tests for core functionality
- Integration tests with real etcd
- Event system testing
- Configuration type conversion testing 
- Error condition handling

## Documentation

See [documentation](https://docs.ergo.services/extra-library/registrars/etcd-client) for complete usage guide and examples. 
