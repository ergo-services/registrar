package etcd

import (
	"crypto/tls"
	"fmt"
	"sync"
	"time"

	"ergo.services/ergo/gen"
	etcdcli "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

const (
	pathPrefix              = "services/ergo"
	formatPathCluster       = pathPrefix + "/cluster/%s/"
	formatPathClusterRoutes = pathPrefix + "/cluster/%s/routes/" // Non-overlapping with config
	formatPathNodes         = pathPrefix + "/cluster/%s/routes/nodes/"
	formatPathApps          = pathPrefix + "/cluster/%s/routes/applications/"
	formatPathConfig        = pathPrefix + "/cluster/%s/config/"
	formatPathGlobalConfig  = pathPrefix + "/config/"

	// Default configuration values
	defaultDialTimeout    = 10 * time.Second
	defaultRequestTimeout = 10 * time.Second
	defaultKeepAlive      = 10 * time.Second
)

// Configuration Key Format:
// Configuration items are stored in etcd using hierarchical paths with the following formats:
//
// Within each cluster's config path (services/ergo/cluster/{cluster}/config/):
//
// 1. Node-specific within cluster: "node/item"
//    Example: "web1/database.timeout"
//    etcd path: services/ergo/cluster/production/config/web1/database.timeout
//
// 2. Cluster-wide default: "*/item"
//    Example: "*/log.level"
//    etcd path: services/ergo/cluster/production/config/*/log.level
//
// For cross-cluster configurations (services/ergo/config/):
//
// 3. Cross-cluster node-specific: "cluster/node/item"
//    Example: "production/web1/database.host"
//    etcd path: services/ergo/config/production/web1/database.host
//
// 4. Global default: "global/item"
//    Example: "global/debug.enabled"
//    etcd path: services/ergo/config/global/debug.enabled
//
// Resolution priority (highest to lowest): 3 -> 1 -> 2 -> 4

var (
	defaultEndpoins = []string{"localhost:2379"}
)

type client struct {
	options Options

	cli   *etcdcli.Client
	lease etcdcli.LeaseID

	node gen.NodeRegistrar

	pathCluster       string
	pathClusterRoutes string // Non-overlapping with config - uses edf.Encode + base64
	pathNodes         string
	pathApps          string
	pathConfig        string // Uses string encoding with type prefixes
	pathGlobalConfig  string

	routes []gen.Route

	config     map[string]any
	configLock sync.RWMutex
	apps       sync.Map // map[gen.Atom]gen.ApplicationRoute

	event    gen.Event
	eventRef gen.Ref

	state int32 // 0 unregistered, 1 registered, 2 terminated
}

// Options for ETCD registrar with authentication and security support
type Options struct {
	Cluster   string
	Endpoints []string

	// Authentication options
	Username string
	Password string

	// TLS/Security options
	TLS                *tls.Config
	InsecureSkipVerify bool

	// Connection options
	DialTimeout    time.Duration
	RequestTimeout time.Duration
	KeepAlive      time.Duration
}

func Create(options Options) (gen.Registrar, error) {
	if len(options.Endpoints) == 0 {
		options.Endpoints = defaultEndpoins
	}

	if options.Cluster == "" {
		options.Cluster = "default"
	}

	if options.DialTimeout == 0 {
		options.DialTimeout = defaultDialTimeout
	}

	if options.RequestTimeout == 0 {
		options.RequestTimeout = defaultRequestTimeout
	}

	if options.KeepAlive == 0 {
		options.KeepAlive = defaultKeepAlive
	}

	// Build etcd client configuration
	etcdloglevel := zap.NewAtomicLevelAt(zap.ErrorLevel)
	config := etcdcli.Config{
		Endpoints:            options.Endpoints,
		DialTimeout:          options.DialTimeout,
		DialKeepAliveTimeout: options.KeepAlive,
		DialKeepAliveTime:    options.KeepAlive,
		MaxCallSendMsgSize:   2 * 1024 * 1024, // 2MB
		MaxCallRecvMsgSize:   4 * 1024 * 1024, // 4MB
		RejectOldCluster:     true,
		LogConfig: &zap.Config{
			Level:         etcdloglevel,
			Encoding:      "json",
			EncoderConfig: zap.NewProductionEncoderConfig(),
		},
	}

	// Configure authentication
	if options.Username != "" && options.Password != "" {
		config.Username = options.Username
		config.Password = options.Password
	}

	// Configure TLS
	if options.TLS != nil {
		config.TLS = options.TLS
	} else if options.InsecureSkipVerify {
		config.TLS = &tls.Config{
			InsecureSkipVerify: true,
		}
	}

	cli, err := etcdcli.New(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create etcd client: %w", err)
	}

	client := &client{
		options:           options,
		cli:               cli,
		pathCluster:       fmt.Sprintf(formatPathCluster, options.Cluster),
		pathClusterRoutes: fmt.Sprintf(formatPathClusterRoutes, options.Cluster),
		pathNodes:         fmt.Sprintf(formatPathNodes, options.Cluster),
		pathApps:          fmt.Sprintf(formatPathApps, options.Cluster),
		pathConfig:        fmt.Sprintf(formatPathConfig, options.Cluster),
		pathGlobalConfig:  formatPathGlobalConfig,
		config:            make(map[string]any),
	}

	return client, nil
}

// Resolution priority (highest to lowest): 1 -> 2 -> 3 -> 4
//
// Example Usage:
// To set database.host for web1 node in production cluster:
//   etcdctl put services/ergo/cluster/production/config/web1/database.host "db.prod.com"
//
// To set default log level for all nodes in production:
//   etcdctl put services/ergo/cluster/production/config/*/log.level "info"
//
// To set cross-cluster setting for web1 in production:
//   etcdctl put services/ergo/config/production/web1/cache.size "256MB"
//
// To set global debug flag:
//   etcdctl put services/ergo/config/global/debug.enabled "false"
